// sg-pubsub/pkg/clientlib/brokerpubsublib/client.go
package brokerpubsublib

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type Client struct {
	BaseURL    string
	HttpClient *http.Client
	Token      string
	ApiKey     string
}

type PullResponse struct {
	Message Message `json:"message"`
}

type ListTopicsResponse struct {
	Topics []string `json:"topics"`
}

type GetMessagesResponse struct {
	Messages []Message `json:"messages"`
}

type Topic struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Subscription struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Message struct {
	ID        string `json:"id"`
	Data      string `json:"data"`
	Timestamp string `json:"timestamp"`
}

func NewClient(baseURL string, token string, apiKey string, httpClient ...*http.Client) *Client {
	var client *http.Client
	if len(httpClient) > 0 {
		client = httpClient[0]
	} else {
		client = &http.Client{
			Timeout: time.Second * 10,
		}
	}

	return &Client{
		BaseURL:    baseURL,
		HttpClient: client,
		Token:      token,
		ApiKey:     apiKey,
	}
}