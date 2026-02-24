// handler.go
package main

import (
	"time"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func submitTranscode(c *gin.Context) {
	var req struct {
		InputURL      string   `json:"input_url" binding:"required"`
		OutputProfiles []string `json:"output_profiles" binding:"required"`
		CallbackURL   string   `json:"callback_url"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	jobID := uuid.New().String()
	job := &Job{
		ID:             jobID,
		InputURL:       req.InputURL,
		OutputProfiles: req.OutputProfiles,
		Status:         StatusQueued,
		CreatedAt:      time.Now(),
	}
	mu.Lock()
	jobStore[jobID] = job
	mu.Unlock()

	EnqueueJob(job)

	c.JSON(202, gin.H{
		"job_id": jobID,
		"status": StatusQueued,
	})
}

func getJobStatus(c *gin.Context) {
	jobID := c.Param("id")
	mu.RLock()
	job, exists := jobStore[jobID]
	mu.RUnlock()

	if !exists {
		c.JSON(404, gin.H{"error": "job not found"})
		return
	}
	c.JSON(200, job)
}