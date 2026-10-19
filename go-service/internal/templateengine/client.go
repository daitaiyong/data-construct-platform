package templateengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/daitaiyong/data-construct-platform/go-service/internal/pb"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/template"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	ErrInvalidResponse = errors.New("template engine returned invalid response")
	ErrGeneration      = errors.New("template engine failed to generate data")
)

type Previewer interface {
	Preview(ctx context.Context, item template.Template, count uint32, contextData json.RawMessage) ([]json.RawMessage, error)
}

type Client struct {
	connection *grpc.ClientConn
	service    pb.TemplateEngineServiceClient
}

func Dial(address string) (*Client, error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{
		connection: connection,
		service:    pb.NewTemplateEngineServiceClient(connection),
	}, nil
}

func (c *Client) Close() error {
	return c.connection.Close()
}

func (c *Client) Preview(ctx context.Context, item template.Template, count uint32, contextData json.RawMessage) ([]json.RawMessage, error) {
	templateJSON, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}
	if len(contextData) == 0 {
		contextData = json.RawMessage(`{}`)
	}
	response, err := c.service.GenerateData(ctx, &pb.GenerateRequest{
		TemplateJson: string(templateJSON),
		ContextJson:  string(contextData),
		Count:        int64(count),
	})
	if err != nil {
		return nil, err
	}
	if !response.Success {
		return nil, fmt.Errorf("%w: %s", ErrGeneration, response.ErrorMsg)
	}
	if response.Total != int64(len(response.RowsJson)) {
		return nil, fmt.Errorf("%w: total %d does not match %d rows", ErrInvalidResponse, response.Total, len(response.RowsJson))
	}
	rows := make([]json.RawMessage, len(response.RowsJson))
	for index, row := range response.RowsJson {
		if !json.Valid([]byte(row)) {
			return nil, fmt.Errorf("%w: row %d is not valid JSON", ErrInvalidResponse, index)
		}
		rows[index] = json.RawMessage(row)
	}
	return rows, nil
}

func (c *Client) ValidateTemplate(ctx context.Context, item template.Template) (*pb.ValidateResponse, error) {
	templateJSON, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}
	return c.service.ValidateTemplate(ctx, &pb.ValidateRequest{TemplateJson: string(templateJSON)})
}
