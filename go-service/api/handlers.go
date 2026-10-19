package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/daitaiyong/data-construct-platform/go-service/internal/dag"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/id"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/job"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/template"
)

type Handler struct {
	templates template.Repository
	jobs      job.Repository
	previewer Previewer
}

type Previewer interface {
	Preview(ctx context.Context, item template.Template, count uint32, contextData json.RawMessage) ([]json.RawMessage, error)
}

func NewHandler(templates template.Repository, jobs job.Repository, previewers ...Previewer) *Handler {
	handler := &Handler{templates: templates, jobs: jobs}
	if len(previewers) > 0 {
		handler.previewer = previewers[0]
	}
	return handler
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.health)
	mux.HandleFunc("GET /api/v1/template", h.listTemplates)
	mux.HandleFunc("POST /api/v1/template", h.createTemplate)
	mux.HandleFunc("GET /api/v1/template/{template_id}", h.getTemplate)
	mux.HandleFunc("PUT /api/v1/template/{template_id}", h.updateTemplate)
	mux.HandleFunc("DELETE /api/v1/template/{template_id}", h.deleteTemplate)
	mux.HandleFunc("POST /api/v1/template/preview", h.previewTemplate)
	mux.HandleFunc("POST /api/v1/dags/validate", h.validateDAG)
	mux.HandleFunc("POST /api/v1/jobs", h.createJob)
	mux.HandleFunc("GET /api/v1/jobs/{id}", h.getJob)
	return withJSONContentType(mux)
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) listTemplates(w http.ResponseWriter, r *http.Request) {
	page, err := parsePositiveQuery(r, "page", 1)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	size, err := parsePositiveQuery(r, "size", 10)
	if err != nil || size > 100 {
		writeError(w, http.StatusBadRequest, "size must be between 1 and 100")
		return
	}
	items, total := h.templates.List(page, size, r.URL.Query().Get("name"))
	writeJSON(w, http.StatusOK, map[string]any{
		"data": map[string]any{
			"items": items,
			"total": total,
			"page":  page,
			"size":  size,
		},
	})
}

func (h *Handler) createTemplate(w http.ResponseWriter, r *http.Request) {
	var request template.Template
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateTemplate(request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	request.ID = ""
	item, err := h.templates.Create(request)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to create template")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"data": map[string]string{"template_id": item.ID}})
}

func (h *Handler) getTemplate(w http.ResponseWriter, r *http.Request) {
	templateID := r.PathValue("template_id")
	if !writeInvalidID(w, templateID, "template_id") {
		return
	}
	item, err := h.templates.Get(templateID)
	if errors.Is(err, template.ErrNotFound) {
		writeError(w, http.StatusNotFound, "template not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to load template")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": item})
}

func (h *Handler) updateTemplate(w http.ResponseWriter, r *http.Request) {
	templateID := r.PathValue("template_id")
	if !writeInvalidID(w, templateID, "template_id") {
		return
	}
	var request template.Template
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateTemplate(request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	item, err := h.templates.Update(templateID, request)
	if errors.Is(err, template.ErrNotFound) {
		writeError(w, http.StatusNotFound, "template not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to update template")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": item})
}

func (h *Handler) deleteTemplate(w http.ResponseWriter, r *http.Request) {
	templateID := r.PathValue("template_id")
	if !writeInvalidID(w, templateID, "template_id") {
		return
	}
	err := h.templates.Delete(templateID)
	if errors.Is(err, template.ErrNotFound) {
		writeError(w, http.StatusNotFound, "template not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to delete template")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": map[string]bool{"deleted": true}})
}

func (h *Handler) previewTemplate(w http.ResponseWriter, r *http.Request) {
	if h.previewer == nil {
		writeError(w, http.StatusServiceUnavailable, "template engine is not configured")
		return
	}
	var request struct {
		TemplateID string          `json:"template_id"`
		Count      uint32          `json:"count"`
		Context    json.RawMessage `json:"context"`
	}
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if request.TemplateID == "" {
		writeError(w, http.StatusBadRequest, "template_id is required")
		return
	}
	if !writeInvalidID(w, request.TemplateID, "template_id") {
		return
	}
	if request.Count == 0 || request.Count > 100 {
		writeError(w, http.StatusBadRequest, "count must be between 1 and 100")
		return
	}
	if len(request.Context) == 0 {
		request.Context = json.RawMessage(`{}`)
	}
	var contextObject map[string]any
	if !json.Valid(request.Context) || json.Unmarshal(request.Context, &contextObject) != nil || contextObject == nil {
		writeError(w, http.StatusBadRequest, "context must be a JSON object")
		return
	}
	item, err := h.templates.Get(request.TemplateID)
	if errors.Is(err, template.ErrNotFound) {
		writeError(w, http.StatusNotFound, "template not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to load template")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	rows, err := h.previewer.Preview(ctx, item, request.Count, request.Context)
	if err != nil {
		log.Printf("preview template %s: %v", item.ID, err)
		writeError(w, http.StatusBadGateway, "template engine failed to generate preview")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"rows": rows}})
}

func (h *Handler) validateDAG(w http.ResponseWriter, r *http.Request) {
	var request dag.Content
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	order, err := dag.TopologicalOrder(request)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string][]string{"order": order})
}

func (h *Handler) createJob(w http.ResponseWriter, r *http.Request) {
	var request struct {
		DAGID       string `json:"dag_id"`
		TriggerType string `json:"trigger_type"`
	}
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(request.DAGID) == "" {
		writeError(w, http.StatusBadRequest, "dag_id is required")
		return
	}
	if !writeInvalidID(w, request.DAGID, "dag_id") {
		return
	}
	if request.TriggerType == "" {
		request.TriggerType = "api"
	}
	if request.TriggerType != "manual" && request.TriggerType != "schedule" && request.TriggerType != "api" {
		writeError(w, http.StatusBadRequest, "trigger_type must be manual, schedule, or api")
		return
	}
	item, err := h.jobs.Create(request.DAGID, request.TriggerType)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to create job")
		return
	}
	writeJSON(w, http.StatusAccepted, item)
}

func validateTemplate(item template.Template) error {
	if strings.TrimSpace(item.Name) == "" ||
		strings.TrimSpace(item.Description) == "" ||
		strings.TrimSpace(item.TargetType) == "" ||
		strings.TrimSpace(item.TargetTable) == "" ||
		strings.TrimSpace(item.SinkConf.DBKey) == "" ||
		item.BatchSize == 0 ||
		len(item.Fields) == 0 {
		return errors.New("name, desc, target_type, target_table, positive batch_size, fields, and sink_conf.db_key are required")
	}
	fieldNames := make(map[string]struct{}, len(item.Fields))
	for _, field := range item.Fields {
		if strings.TrimSpace(field.Name) == "" || strings.TrimSpace(field.Type) == "" {
			return errors.New("each field requires name and type")
		}
		if _, exists := fieldNames[field.Name]; exists {
			return errors.New("field names must be unique")
		}
		fieldNames[field.Name] = struct{}{}
	}
	return nil
}

func parsePositiveQuery(r *http.Request, key string, fallback int) (int, error) {
	value := r.URL.Query().Get(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return 0, errors.New(key + " must be a positive integer")
	}
	return parsed, nil
}

func (h *Handler) getJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if !writeInvalidID(w, jobID, "job_id") {
		return
	}
	item, err := h.jobs.Get(jobID)
	if errors.Is(err, job.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to load job")
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func writeInvalidID(w http.ResponseWriter, value, name string) bool {
	if id.Valid(value) {
		return true
	}
	writeError(w, http.StatusBadRequest, name+" must be a Snowflake ID")
	return false
}

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request must contain a single JSON value")
		}
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write JSON response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func withJSONContentType(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		next.ServeHTTP(w, r)
	})
}
