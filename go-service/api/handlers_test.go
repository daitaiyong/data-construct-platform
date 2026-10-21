package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daitaiyong/data-construct-platform/go-service/internal/job"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/kafka"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/template"
)

type fakePreviewer struct {
	rows []json.RawMessage
}

func (f fakePreviewer) Preview(context.Context, template.Template, uint32, json.RawMessage) ([]json.RawMessage, error) {
	return f.rows, nil
}

type fakeEventPublisher struct {
	topic   string
	message kafka.Message
}

func (p *fakeEventPublisher) Publish(_ context.Context, topic string, message kafka.Message) error {
	p.topic = topic
	p.message = message
	return nil
}

func TestValidateDAG(t *testing.T) {
	handler := NewHandler(template.NewMemoryRepository(), job.NewMemoryRepository())
	request := httptest.NewRequest(http.MethodPost, "/api/v1/dags/validate",
		strings.NewReader(`{"nodes":[{"node_id":"b","node_name":"B","template_id":"tpl-b","position":{"x":500,"y":0},"config":{"batch_size":2,"fail_strategy":"abort_dag"}},{"node_id":"a","node_name":"A","template_id":"tpl-a","position":{"x":0,"y":0},"config":{"batch_size":1,"fail_strategy":"continue"}}],"edges":[{"edge_id":"e1","source_node_id":"a","target_node_id":"b"}],"dag_meta":{"version":"v1","parallel":true}}`))
	response := httptest.NewRecorder()
	handler.Routes().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
	}
	if !strings.Contains(response.Body.String(), `"order":["a","b"]`) {
		t.Fatalf("unexpected response: %s", response.Body)
	}
}

func TestValidateDAGRejectsCycle(t *testing.T) {
	handler := NewHandler(template.NewMemoryRepository(), job.NewMemoryRepository())
	body := `{"nodes":[{"node_id":"a","node_name":"A","template_id":"tpl-a","config":{"batch_size":1,"fail_strategy":"continue"}},{"node_id":"b","node_name":"B","template_id":"tpl-b","config":{"batch_size":1,"fail_strategy":"continue"}}],"edges":[{"edge_id":"e1","source_node_id":"a","target_node_id":"b"},{"edge_id":"e2","source_node_id":"b","target_node_id":"a"}],"dag_meta":{"version":"v1","parallel":false}}`
	response := serve(handler.Routes(), http.MethodPost, "/api/v1/dags/validate", body)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusUnprocessableEntity, response.Body)
	}
}

func TestTemplateCRUDAndPreview(t *testing.T) {
	repository := template.NewMemoryRepository()
	previewer := fakePreviewer{rows: []json.RawMessage{json.RawMessage(`{"id":"1"}`)}}
	handler := NewHandler(repository, job.NewMemoryRepository(), previewer).Routes()
	templateJSON := `{"name":"用户表模板","desc":"生成用户数据","target_type":"mysql","target_table":"test_user","batch_size":1000,"fields":[{"name":"id","type":"snowflake","nullable":false,"rule":"snowflake()","comment":"主键"}],"sink_conf":{"db_key":"test_mysql_01","truncate_before":false}}`

	createResponse := serve(handler, http.MethodPost, "/api/v1/template", templateJSON)
	if createResponse.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d: %s", createResponse.Code, http.StatusCreated, createResponse.Body)
	}
	var created struct {
		Data struct {
			TemplateID string `json:"template_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(createResponse.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Data.TemplateID == "" {
		t.Fatal("create response omitted data.template_id")
	}

	detailPath := "/api/v1/template/" + created.Data.TemplateID
	if response := serve(handler, http.MethodGet, detailPath, ""); response.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
	}
	if response := serve(handler, http.MethodGet, "/api/v1/template?page=1&size=10&name=用户", ""); response.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
	}
	if response := serve(handler, http.MethodPost, "/api/v1/template/preview",
		`{"template_id":"`+created.Data.TemplateID+`","count":1,"context":{}}`); response.Code != http.StatusOK {
		t.Fatalf("preview status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
	} else if !strings.Contains(response.Body.String(), `"rows":[{"id":"1"}]`) {
		t.Fatalf("preview response omitted sample rows: %s", response.Body)
	}
	if response := serve(handler, http.MethodPut, detailPath, templateJSON); response.Code != http.StatusOK {
		t.Fatalf("update status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
	}
	if response := serve(handler, http.MethodDelete, detailPath, ""); response.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
	}
	if response := serve(handler, http.MethodGet, detailPath, ""); response.Code != http.StatusNotFound {
		t.Fatalf("get after delete status = %d, want %d", response.Code, http.StatusNotFound)
	}
}

func TestCreateJobUsesDAGMetadata(t *testing.T) {
	handler := NewHandler(template.NewMemoryRepository(), job.NewMemoryRepository())
	response := serve(handler.Routes(), http.MethodPost, "/api/v1/jobs",
		`{"dag_id":"1700000000000000000","trigger_type":"manual"}`)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusAccepted, response.Body)
	}
	body := response.Body.String()
	for _, expected := range []string{`"dag_id":"1700000000000000000"`, `"trigger_type":"manual"`, `"context_key":"dc:job:`} {
		if !strings.Contains(body, expected) {
			t.Fatalf("response %q does not contain %q", body, expected)
		}
	}
}

func TestCancelJobPublishesCancellationEvent(t *testing.T) {
	publisher := &fakeEventPublisher{}
	jobRepository := job.NewMemoryRepository()
	created, err := jobRepository.Create("1700000000000000000", "manual")
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandlerWithEventPublisher(template.NewMemoryRepository(), jobRepository, nil, publisher).Routes()
	jobID := created.ID
	response := serve(handler, http.MethodPost, "/api/v1/jobs/"+jobID+"/cancel", "")
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusAccepted, response.Body)
	}
	if publisher.topic != kafka.EventTopic || string(publisher.message.Key) != jobID {
		t.Fatalf("published topic/key = %q/%q", publisher.topic, publisher.message.Key)
	}
	event, err := kafka.DecodeEventMessage(publisher.message.Value)
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != kafka.EventJobCancel || event.JobID != jobID {
		t.Fatalf("published event = %#v", event)
	}
}

func serve(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
