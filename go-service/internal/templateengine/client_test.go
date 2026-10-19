package templateengine

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	"github.com/daitaiyong/data-construct-platform/go-service/internal/pb"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/template"
	"google.golang.org/grpc"
)

type testTemplateEngineServer struct {
	pb.UnimplementedTemplateEngineServiceServer
	t *testing.T
}

func (s testTemplateEngineServer) GenerateData(_ context.Context, request *pb.GenerateRequest) (*pb.GenerateResponse, error) {
	if request.Count != 2 {
		s.t.Errorf("count = %d, want 2", request.Count)
	}
	var contextData map[string]any
	if err := json.Unmarshal([]byte(request.ContextJson), &contextData); err != nil {
		s.t.Errorf("context_json is invalid: %v", err)
	}
	return &pb.GenerateResponse{Success: true, RowsJson: []string{`{"id":"1"}`, `{"id":"2"}`}, Total: 2}, nil
}

func (testTemplateEngineServer) ValidateTemplate(context.Context, *pb.ValidateRequest) (*pb.ValidateResponse, error) {
	return &pb.ValidateResponse{Ok: true}, nil
}

func TestClientCallsGenerateAndValidateMethods(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterTemplateEngineServiceServer(server, testTemplateEngineServer{t: t})
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil {
			t.Errorf("serve gRPC test server: %v", serveErr)
		}
	}()
	defer server.Stop()

	client, err := Dial(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	rows, err := client.Preview(ctx, template.Template{}, 2, json.RawMessage(`{"user_ids":["1"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || string(rows[0]) != `{"id":"1"}` {
		t.Fatalf("Preview() = %s, want two JSON rows", rows)
	}

	response, err := client.ValidateTemplate(ctx, template.Template{})
	if err != nil {
		t.Fatal(err)
	}
	if !response.Ok {
		t.Fatalf("ValidateTemplate().Ok = false, errors: %v", response.Errors)
	}
}

func TestPreviewReturnsGenerationFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterTemplateEngineServiceServer(server, generationFailureServer{})
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil {
			t.Errorf("serve gRPC test server: %v", serveErr)
		}
	}()
	defer server.Stop()

	client, err := Dial(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, err = client.Preview(context.Background(), template.Template{}, 1, nil)
	if err == nil {
		t.Fatal("Preview() error = nil, want generation error")
	}
}

type generationFailureServer struct {
	pb.UnimplementedTemplateEngineServiceServer
}

func (generationFailureServer) GenerateData(context.Context, *pb.GenerateRequest) (*pb.GenerateResponse, error) {
	return &pb.GenerateResponse{Success: false, ErrorMsg: "invalid rule"}, nil
}

func (generationFailureServer) ValidateTemplate(context.Context, *pb.ValidateRequest) (*pb.ValidateResponse, error) {
	return &pb.ValidateResponse{}, nil
}
