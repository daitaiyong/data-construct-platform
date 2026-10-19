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
			strings.NewReader(`{"name":"sample","content":"hello {name}"}`))
		response := httptest.NewRecorder()
		handler.Routes().ServeHTTP(response, request)
		if response.Code != http.StatusCreated {
			t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusCreated, response.Body)
		}
		if !strings.Contains(response.Body.String(), `"name":"sample"`) {
			t.Fatalf("unexpected response: %s", response.Body)
		}
	}
	if !strings.Contains(response.Body.String(), `"order":["a","b"]`) {
		t.Fatalf("unexpected response: %s", response.Body)
	}
}
