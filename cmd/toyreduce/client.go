package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/dustin/go-humanize"
	"pkg.jsn.cam/toyreduce/pkg/toyreduce/protocol"
)

func submitJobHTTP(masterURL, executor, path string, chunkSize, reduceTasks int) {
	req := protocol.JobSubmitRequest{
		Executor:    executor,
		InputPath:   path,
		ChunkSize:   chunkSize,
		ReduceTasks: reduceTasks,
	}

	body, err := json.Marshal(req)
	if err != nil {
		log.Fatalf("Failed to marshal request: %v", err)
	}

	resp, err := http.Post(masterURL+"/api/jobs", "application/json", bytes.NewBuffer(body))
	if err != nil {
		log.Fatalf("Failed to submit job: %v", err)
	}
	defer resp.Body.Close()

	var submitResp protocol.JobSubmitResponse
	if err := json.NewDecoder(resp.Body).Decode(&submitResp); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	if submitResp.Status == "error" {
		log.Fatalf("Job submission failed: %s", submitResp.Message)
	}

	fmt.Printf("Job submitted successfully!\n")
	fmt.Printf("  Job ID: %s\n", submitResp.JobID)
	fmt.Printf("  Status: %s\n", submitResp.Status)
	fmt.Printf("\nCheck status with: toyreduce status --job-id %s\n", submitResp.JobID)
}

func listJobsHTTP(masterURL string) {
	resp, err := http.Get(masterURL + "/api/jobs")
	if err != nil {
		log.Fatalf("Failed to list jobs: %v", err)
	}
	defer resp.Body.Close()

	var listResp protocol.JobListResponse
	if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	if len(listResp.Jobs) == 0 {
		fmt.Println("No jobs found")
		return
	}

	fmt.Printf("%-36s %-12s %-15s %s\n", "JOB ID", "STATUS", "EXECUTOR", "SUBMITTED")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────────────")
	for _, job := range listResp.Jobs {
		fmt.Printf("%-36s %-12s %-15s %s\n",
			job.ID,
			job.Status,
			job.Executor,
			job.SubmittedAt.Format("2006-01-02 15:04:05"))
	}
}

func getJobStatusHTTP(masterURL, jobID string) {
	resp, err := http.Get(masterURL + "/api/jobs/" + jobID)
	if err != nil {
		log.Fatalf("Failed to get job status: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		log.Fatalf("Job not found: %s", jobID)
	}

	var job protocol.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	// Convert chunk size MB to human-readable format
	chunkSizeBytes := uint64(job.ChunkSize) * 1024 * 1024
	chunkSizeHuman := humanize.Bytes(chunkSizeBytes)

	fmt.Printf("Job Details:\n")
	fmt.Printf("  ID:          %s\n", job.ID)
	fmt.Printf("  Status:      %s\n", job.Status)
	fmt.Printf("  Executor:    %s\n", job.Executor)
	fmt.Printf("  Input Path:  %s\n", job.InputPath)
	fmt.Printf("  Chunk Size:  %s\n", chunkSizeHuman)
	fmt.Printf("  Reduce Tasks: %d\n", job.ReduceTasks)
	fmt.Printf("  Submitted:   %s\n", job.SubmittedAt.Format("2006-01-02 15:04:05"))

	if !job.StartedAt.IsZero() {
		fmt.Printf("  Started:     %s\n", job.StartedAt.Format("2006-01-02 15:04:05"))
	}

	if !job.CompletedAt.IsZero() {
		fmt.Printf("  Completed:   %s\n", job.CompletedAt.Format("2006-01-02 15:04:05"))
		duration := job.CompletedAt.Sub(job.StartedAt)
		fmt.Printf("  Duration:    %v\n", duration)
	}

	if job.Status == protocol.JobStatusRunning || job.Status == protocol.JobStatusCompleted {
		fmt.Printf("\nProgress:\n")
		fmt.Printf("  Map tasks:    %d/%d completed\n", job.MapTasksDone, job.MapTasksTotal)
		fmt.Printf("  Reduce tasks: %d/%d completed\n", job.ReduceTasksDone, job.ReduceTasksTotal)
		fmt.Printf("  Total:        %d/%d completed\n", job.CompletedTasks, job.TotalTasks)
	}

	if job.Error != "" {
		fmt.Printf("\nError: %s\n", job.Error)
	}
}

func getJobResultsHTTP(masterURL, jobID string) {
	resp, err := http.Get(masterURL + "/api/jobs/" + jobID + "/results")
	if err != nil {
		log.Fatalf("Failed to get job results: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Failed to get results (status: %d)", resp.StatusCode)
	}

	var results []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	if len(results) == 0 {
		fmt.Println("No results yet")
		return
	}

	fmt.Printf("Job Results (%d entries):\n", len(results))
	fmt.Println("─────────────────────────────────────────────────────────")
	for _, kv := range results {
		fmt.Printf("%-30s %s\n", kv.Key, kv.Value)
	}
}

func cancelJobHTTP(masterURL, jobID string) {
	resp, err := http.Post(masterURL+"/api/jobs/"+jobID+"/cancel", "application/json", nil)
	if err != nil {
		log.Fatalf("Failed to cancel job: %v", err)
	}
	defer resp.Body.Close()

	var cancelResp protocol.JobCancelResponse
	if err := json.NewDecoder(resp.Body).Decode(&cancelResp); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	if !cancelResp.Success {
		log.Fatalf("Failed to cancel job: %s", cancelResp.Message)
	}

	fmt.Printf("Job cancelled successfully: %s\n", jobID)
}
