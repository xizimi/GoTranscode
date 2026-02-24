// job.go
package main

import "time"

type JobStatus string

const (
	StatusQueued     JobStatus = "queued"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
)

type Job struct {
	ID            string     `json:"job_id"`
	InputURL      string     `json:"input_url"`
	OutputProfiles []string  `json:"output_profiles"`
	Status        JobStatus  `json:"status"`
	CreatedAt     time.Time  `json:"created_at"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
	OutputPaths   []string   `json:"outputs,omitempty"`
}

var jobStore = make(map[string]*Job)