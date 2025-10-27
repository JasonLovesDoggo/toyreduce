package worker

import (
	"bytes"
	"context"
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
	http      *http.Client
	masterURL string
	storeURL  string
}

// NewClient creates a new worker client
func NewClient(masterURL string) *Client {
	return &Client{
		masterURL: masterURL,
		http: &http.Client{
			// Add default timeout as safety net to prevent indefinite hangs.
			// Per-request timeouts can be enforced via context deadlines which will override this.
			// This ensures that requests don't hang if no context deadline is set.
			Timeout: 30 * time.Second,
		},
	}
}

// Register registers this worker with the master
func (c *Client) Register(ctx context.Context, workerID string, version string, executors []string, dataEndpoint string) (*protocol.WorkerRegistrationResponse, error) {
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

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.masterURL+"/api/workers/register", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
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
		return nil, fmt.Errorf("%w: %s", toyreduce.ErrRegistrationFailed, regResp.Error)
	}

	// Store store URL
	c.storeURL = regResp.StoreURL

	return &regResp, nil
}

// GetNextTask requests the next task from master
func (c *Client) GetNextTask(ctx context.Context, workerID string) (*protocol.Task, error) {
	url := fmt.Sprintf("%s/api/tasks/next?workerID=%s", c.masterURL, workerID)

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %s", toyreduce.ErrGetTaskFailed, resp.Status)
	}

	var task protocol.Task
	if err := json.NewDecoder(resp.Body).Decode(&task); err != nil {
		return nil, err
	}

	return &task, nil
}

// CompleteTask notifies master that a task is complete
func (c *Client) CompleteTask(ctx context.Context, taskID, workerID, version string, success bool, errorMsg string) error {
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

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: %s - %s", toyreduce.ErrCompleteTaskFailed, resp.Status, string(bodyBytes))
	}

	return nil
}

// SendHeartbeat sends a heartbeat to master and returns whether it was accepted
func (c *Client) SendHeartbeat(ctx context.Context, workerID string) (bool, error) {
	req := protocol.HeartbeatRequest{
		Timestamp: time.Now(),
	}

	body, err := json.Marshal(req)
	if err != nil {
		return false, err
	}

	url := fmt.Sprintf("%s/api/workers/%s/heartbeat", c.masterURL, workerID)

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(body))
	if err != nil {
		return false, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("%w: %s", toyreduce.ErrHeartbeatFailed, resp.Status)
	}

	var hbResp protocol.HeartbeatResponse
	if err := json.NewDecoder(resp.Body).Decode(&hbResp); err != nil {
		return false, fmt.Errorf("failed to decode heartbeat response: %w", err)
	}

	return hbResp.OK, nil
}

// StoreMapOutput sends map output to store
func (c *Client) StoreMapOutput(ctx context.Context, taskID string, partition int, data []toyreduce.KeyValue) error {
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

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: %s", toyreduce.ErrStoreMapOutputFailed, resp.Status)
	}

	return nil
}

// GetReduceInput retrieves intermediate data for a partition from store
func (c *Client) GetReduceInput(ctx context.Context, partition int) ([]toyreduce.KeyValue, error) {
	url := fmt.Sprintf("%s/intermediate/reduce/%d", c.storeURL, partition)

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %s", toyreduce.ErrGetReduceInputFailed, resp.Status)
	}

	var data []toyreduce.KeyValue
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	return data, nil
}

// StoreReduceOutput sends reduce output to store
func (c *Client) StoreReduceOutput(ctx context.Context, taskID, jobID string, data []toyreduce.KeyValue) error {
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

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: %s", toyreduce.ErrStoreReduceOutputFailed, resp.Status)
	}

	return nil
}

// FetchPartitionFromWorker fetches a specific partition from a worker's data endpoint
func (c *Client) FetchPartitionFromWorker(ctx context.Context, workerEndpoint, jobID string, partition int) ([]toyreduce.KeyValue, error) {
	url := fmt.Sprintf("%s/data/%s/partition/%d", workerEndpoint, jobID, partition)

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("fetch from worker %s: %w", workerEndpoint, err)
	}

	resp, err := c.http.Do(httpReq)
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
func (c *Client) RequestCleanupFromWorker(ctx context.Context, workerEndpoint, jobID string) error {
	url := fmt.Sprintf("%s/cleanup/%s", workerEndpoint, jobID)

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return fmt.Errorf("cleanup request to worker %s: %w", workerEndpoint, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return fmt.Errorf("cleanup request to worker %s: %w", workerEndpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("worker %s returned status %d", workerEndpoint, resp.StatusCode)
	}

	return nil
}
