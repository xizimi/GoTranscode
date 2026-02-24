package main

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func setupRouter() *gin.Engine {
	r := gin.Default()
	r.POST("/transcode", transcodeHandler)
	r.GET("/health", healthHandler)
	return r
}

func transcodeHandler(c *gin.Context) {
	var req struct {
		InputPath      string   `json:"input_path"`
		OutputProfiles []string `json:"output_profiles"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	job := Job{
		InputPath:      req.InputPath,
		OutputProfiles: req.OutputProfiles,
	}

	// 关键修改：使用 Kafka AddJob 替代内存队列
	if err := AddJob(job); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to queue job"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Job queued successfully"})
}

func healthHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "healthy"})
}