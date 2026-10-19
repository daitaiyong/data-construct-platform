package api

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/daitaiyong/data-construct-platform/go-service/internal/dag"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/job"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/template"
)

type Handler struct {
	templates template.Repository
	jobs      job.Repository
}

func NewHandler(templates template.Repository, jobs job.Repository) *Handler {
	return &Handler{templates: templates, jobs: jobs}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.health)
	mux.HandleFunc("GET /api/v1/templates", h.listTemplates)
	mux.HandleFunc("POST /api/v1/templates", h.createTemplate)
	mux.HandleFunc("POST /api/v1/dags/validate", h.validateDAG)
	mux.HandleFunc("POST /api/v1/jobs", h.createJob)
	mux.HandleFunc("GET /api/v1/jobs/{id}", h.getJob)
	return withJSONContentType(mux)
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) listTemplates(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.templates.List())
}

func (h *Handler) createTemplate(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Name    string `json:"name"`
		Content string `json:"content"`
	}
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(request.Name) == "" || strings.TrimSpace(request.Content) == "" {
		writeError(w, http.StatusBadRequest, "name and content are required")
		return
	}
	item, err := h.templates.Create(request.Name, request.Content)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to create template")
		return
	}
	writeJSON(w, http.StatusCreated, item)
}

func (h *Handler) validateDAG(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Nodes []dag.Node `json:"nodes"`
	}
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	order, err := dag.TopologicalOrder(request.Nodes)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string][]string{"order": order})
}

func (h *Handler) createJob(w http.ResponseWriter, r *http.Request) {
	var request struct {
		TemplateID string `json:"templateId"`
	}
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(request.TemplateID) == "" {
		writeError(w, http.StatusBadRequest, "templateId is required")
		return
	}
	if _, err := h.templates.Get(request.TemplateID); errors.Is(err, template.ErrNotFound) {
		writeError(w, http.StatusNotFound, "template not found")
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to load template")
		return
	}
	item := h.jobs.Create(request.TemplateID)
	writeJSON(w, http.StatusAccepted, item)
}

func (h *Handler) getJob(w http.ResponseWriter, r *http.Request) {
	item, err := h.jobs.Get(r.PathValue("id"))
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
