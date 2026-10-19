package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"data-construct-platform/go-service/internal/job"
	"data-construct-platform/go-service/internal/template"
)

func TestValidateDAG(t *testing.T) {
	handler := NewHandler(template.NewMemoryRepository(), job.NewMemoryRepository())
	request := httptest.NewRequest(http.MethodPost, "/api/v1/dags/validate",
		strings.NewReader(`{"nodes":[{"id":"b","dependencies":["a"]},{"id":"a"}]}`))
	response := httptest.NewRecorder()
	handler.Routes().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
	}

	func TestCreateTemplate(t *testing.T) {
		handler := NewHandler(template.NewMemoryRepository(), job.NewMemoryRepository())
		request := httptest.NewRequest(http.MethodPost, "/api/v1/templates",
			strings.NewReader(`{"name":"sample","desc":"example","targetType":"mysql","batchSize":100,"fields":{"name":{"type":"string"}},"creator":"tester"}`))
		response := httptest.NewRecorder()
		handler.Routes().ServeHTTP(response, request)
		if response.Code != http.StatusCreated {
			t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusCreated, response.Body)
		}
		if !strings.Contains(response.Body.String(), `"name":"sample"`) ||
			!strings.Contains(response.Body.String(), `"batchSize":100`) ||
			!strings.Contains(response.Body.String(), `"fields":{"name":{"type":"string"}}`) {
			t.Fatalf("unexpected response: %s", response.Body)
		}
	}

	func TestCreateJobUsesDAGMetadata(t *testing.T) {
		handler := NewHandler(template.NewMemoryRepository(), job.NewMemoryRepository())
		request := httptest.NewRequest(http.MethodPost, "/api/v1/jobs",
			strings.NewReader(`{"dagId":"dag-1","triggerType":"manual"}`))
		response := httptest.NewRecorder()
		handler.Routes().ServeHTTP(response, request)
		if response.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusAccepted, response.Body)
		}
		body := response.Body.String()
		for _, expected := range []string{`"dagId":"dag-1"`, `"triggerType":"manual"`, `"contextKey":"job:`} {
			if !strings.Contains(body, expected) {
				t.Fatalf("response %q does not contain %q", body, expected)
			}
		}
	}
	if !strings.Contains(response.Body.String(), `"order":["a","b"]`) {
		t.Fatalf("unexpected response: %s", response.Body)
	}
}
