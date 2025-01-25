// ge-pubsub/pkg/clientlib/pubsublib/client.go
package pubsublib

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	validation "github.com/go-ozzo/ozzo-validation"
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

func (cli *Client) CreateTopic(topic Topic) error {
	topicJson, err := json.Marshal(topic)
	if err != nil {
		return err
	}

	// Create a new request
	req, err := http.NewRequest("POST", cli.BaseURL+"/topics", bytes.NewBuffer(topicJson))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cli.Token)
	req.Header.Set("X-API-Key", cli.ApiKey)

	resp, err := cli.HttpClient.Do(req) // execute the request
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("error creating topic: expected status code 201, got %d", resp.StatusCode)
	}
	return nil
}

func (cli *Client) CreateSubscription(topicName string, subscription Subscription) error {
	subscriptionJson, err := json.Marshal(subscription)
	if err != nil {
		return err
	}

	// Create the request
	req, err := http.NewRequest(http.MethodPost, cli.BaseURL+"/topics/"+topicName+"/subscriptions", bytes.NewBuffer(subscriptionJson))
	if err != nil {
		return err
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cli.Token)
	req.Header.Set("X-API-Key", cli.ApiKey)

	// Execute the request
	resp, err := cli.HttpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("error creating subscription: expected status code 201, got %d", resp.StatusCode)
	}
	return nil
}

func (cli *Client) PublishMessage(topicName string, message Message) error {
	messageJson, err := json.Marshal(message)
	if err != nil {
		return err
	}

	// Create a new request
	req, err := http.NewRequest("POST", cli.BaseURL+"/topics/"+topicName+"/publish", bytes.NewBuffer(messageJson))
	if err != nil {
		return err
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cli.Token)
	req.Header.Set("X-API-Key", cli.ApiKey)

	resp, err := cli.HttpClient.Do(req) // execute the request
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("error publishing message: expected status code 201, got %d", resp.StatusCode)
	}
	return nil
}

func (cli *Client) PullMessage(subscriptionName string) (*Message, error) {
	req, err := http.NewRequest("GET", cli.BaseURL+"/subscriptions/"+subscriptionName+"/pull", nil)
	if err != nil {
		return nil, err
	}

	// Set headers
	req.Header.Set("Authorization", "Bearer "+cli.Token)
	req.Header.Set("X-API-Key", cli.ApiKey)

	resp, err := cli.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var pullResponse PullResponse
	err = json.Unmarshal(body, &pullResponse)
	if err != nil {
		return nil, err
	}

	return &pullResponse.Message, nil
}

func (cli *Client) ListTopics() ([]string, error) {
	req, err := http.NewRequest("GET", cli.BaseURL+"/topics", nil)
	if err != nil {
		return nil, err
	}

	// Set headers
	req.Header.Set("Authorization", "Bearer "+cli.Token)
	req.Header.Set("X-API-Key", cli.ApiKey)

	resp, err := cli.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to list topics, status code: %d", resp.StatusCode)
	}

	var listTopicsResponse ListTopicsResponse
	err = json.Unmarshal(body, &listTopicsResponse)
	if err != nil {
		return nil, err
	}

	return listTopicsResponse.Topics, nil
}

func (cli *Client) TopicExists(topic string) (bool, error) {
	err := validation.Validate(topic, validation.Required, validation.Length(1, 255))
	if err != nil {
		return false, fmt.Errorf("invalid topic: %v", err)
	}

	topics, err := cli.ListTopics()
	if err != nil {
		return false, fmt.Errorf("failed to list topics: %v", err)
	}

	for _, t := range topics {
		if t == topic {
			return true, nil
		}
	}

	return false, nil
}

func (cli *Client) EnsureTopicExists(topic string) error {
	err := validation.Validate(topic, validation.Required, validation.Length(1, 255))
	if err != nil {
		return fmt.Errorf("invalid topic: %v", err)
	}

	exists, err := cli.TopicExists(topic)
	if err != nil {
		return err
	}

	if !exists {
		err = cli.CreateTopic(Topic{Name: topic})
		if err != nil {
			return fmt.Errorf("failed to create topic: %v", err)
		}
	}

	return nil
}

func (cli *Client) GetMessages(topic string) ([]Message, error) {
	// Validate the topic
	err := validation.Validate(topic, validation.Required, validation.Length(1, 255))
	if err != nil {
		return nil, fmt.Errorf("invalid topic: %v", err)
	}

	// Create the request to the ge-pubsub API
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/topics/%s/messages", cli.BaseURL, topic), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	// Set headers
	req.Header.Set("Authorization", "Bearer "+cli.Token)
	req.Header.Set("X-API-Key", cli.ApiKey)

	// Send the request and handle the response
	resp, err := cli.HttpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get messages, status code: %d", resp.StatusCode)
	}

	// Decode the response body
	var getMessagesResponse GetMessagesResponse
	err = json.NewDecoder(resp.Body).Decode(&getMessagesResponse)
	if err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	return getMessagesResponse.Messages, nil
}
