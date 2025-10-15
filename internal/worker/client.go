package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"pkg.jsn.cam/toyreduce/pkg/toyreduce"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

// Client handles HTTP communication with master and store
type Client struct {
	masterURL string
	storeURL  string
	http      *http.Client
}

// NewClient creates a new worker client
func NewClient(masterURL string) *Client {
	return &Client{
		masterURL: masterURL,
		http:      &http.Client{},
	}
}

// Register registers this worker with the master
func (c *Client) Register(workerID string, version string, executors []string, dataEndpoint string) (*protocol.WorkerRegistrationResponse, error) {
	req := protocol.WorkerRegistrationRequest{
		WorkerID:     workerID,
		Version:      version,
		Executors:    executors,
		DataEndpoint: dataEndpoint,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Post(
		c.masterURL+"/api/workers/register",
		"application/json",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var regResp protocol.WorkerRegistrationResponse
	if err := json.NewDecoder(resp.Body).Decode(&regResp); err != nil {
		return nil, err
	}

	// Check if registration was successful
	if !regResp.Success {
		return nil, fmt.Errorf("registration failed: %s", regResp.Error)
	}

	// Store store URL
	c.storeURL = regResp.StoreURL

	return &regResp, nil
}

// GetNextTask requests the next task from master
func (c *Client) GetNextTask(workerID string) (*protocol.Task, error) {
	url := fmt.Sprintf("%s/api/tasks/next?workerID=%s", c.masterURL, workerID)

	resp, err := c.http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get task failed: %s", resp.Status)
	}

	var task protocol.Task
	if err := json.NewDecoder(resp.Body).Decode(&task); err != nil {
		return nil, err
	}

	return &task, nil
}

// CompleteTask notifies master that a task is complete
func (c *Client) CompleteTask(taskID, workerID, version string, success bool, errorMsg string) error {
	req := protocol.TaskCompletionRequest{
		Success: success,
		Error:   errorMsg,
		Version: version,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/api/tasks/%s/complete?workerID=%s", c.masterURL, taskID, workerID)

	resp, err := c.http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("complete task failed: %s - %s", resp.Status, string(bodyBytes))
	}

	return nil
}

// SendHeartbeat sends a heartbeat to master
func (c *Client) SendHeartbeat(workerID string) error {
	req := protocol.HeartbeatRequest{
		Timestamp: time.Now(),
	}

	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/api/workers/%s/heartbeat", c.masterURL, workerID)

	resp, err := c.http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("heartbeat failed: %s", resp.Status)
	}

	return nil
}

// StoreMapOutput sends map output to store
func (c *Client) StoreMapOutput(taskID string, partition int, data []toyreduce.KeyValue) error {
	req := protocol.IntermediateData{
		TaskID:    taskID,
		Partition: partition,
		Data:      data,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/intermediate/map/%s/%d", c.storeURL, taskID, partition)

	resp, err := c.http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("store map output failed: %s", resp.Status)
	}

	return nil
}

// GetReduceInput retrieves intermediate data for a partition from store
func (c *Client) GetReduceInput(partition int) ([]toyreduce.KeyValue, error) {
	url := fmt.Sprintf("%s/intermediate/reduce/%d", c.storeURL, partition)

	resp, err := c.http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get reduce input failed: %s", resp.Status)
	}

	var data []toyreduce.KeyValue
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	return data, nil
}

// StoreReduceOutput sends reduce output to store
func (c *Client) StoreReduceOutput(taskID, jobID string, data []toyreduce.KeyValue) error {
	req := protocol.IntermediateData{
		TaskID: taskID,
		JobID:  jobID,
		Data:   data,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/results/%s", c.storeURL, taskID)

	resp, err := c.http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("store reduce output failed: %s", resp.Status)
	}

	return nil
}

// FetchPartitionFromWorker fetches a specific partition from a worker's data endpoint
func (c *Client) FetchPartitionFromWorker(workerEndpoint, jobID string, partition int) ([]toyreduce.KeyValue, error) {
	url := fmt.Sprintf("%s/data/%s/partition/%d", workerEndpoint, jobID, partition)

	resp, err := c.http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetch from worker %s: %w", workerEndpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("worker %s returned status %d", workerEndpoint, resp.StatusCode)
	}

	var data []toyreduce.KeyValue
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("decode response from worker %s: %w", workerEndpoint, err)
	}

	return data, nil
}

// RequestCleanupFromWorker requests a worker to cleanup job data
func (c *Client) RequestCleanupFromWorker(workerEndpoint, jobID string) error {
	url := fmt.Sprintf("%s/cleanup/%s", workerEndpoint, jobID)

	resp, err := c.http.Post(url, "application/json", nil)
	if err != nil {
		return fmt.Errorf("cleanup request to worker %s: %w", workerEndpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("worker %s returned status %d", workerEndpoint, resp.StatusCode)
	}

	return nil
}
