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
	

	nodeTaskCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "node_transcode_tasks_total",
			Help: "Number of transcode tasks completed by node",
		},
		[]string{"node_id", "status"}, // status: completed/failed
	)
	
	// Kafka 队列待处理任务数
	kafkaQueueLength = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_queue_pending_tasks",
			Help: "Number of pending tasks in Kafka queue",
		},
	)
	
	// 节点 CPU 使用率
	nodeCPUUsage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "node_cpu_usage_percent",
			Help: "CPU usage percentage by node",
		},
		[]string{"node_id"},
	)
	
	// 节点内存使用率
	nodeMemoryUsage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "node_memory_usage_percent",
			Help: "Memory usage percentage by node",
		},
		[]string{"node_id"},
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
	// 新增注册
	prometheus.MustRegister(nodeTaskCounter, kafkaQueueLength, nodeCPUUsage, nodeMemoryUsage)
}