// metrics.go
package main

import "github.com/prometheus/client_golang/prometheus"

var (
	taskCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "transcode_tasks_total",
			Help: "Total number of transcode tasks by status",
		},
		[]string{"status"},
	)
	durationHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "transcode_duration_seconds",
			Help:    "Duration of transcode tasks in seconds",
			Buckets: prometheus.ExponentialBuckets(10, 2, 8), // 10s ~ 1280s
		},
	)
)

func init() {
	prometheus.MustRegister(taskCounter, durationHist)
}