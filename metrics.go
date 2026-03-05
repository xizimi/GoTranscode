package main

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
)

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
		[]string{"node_id", "status"},
	)
	
	// RabbitMQ 队列长度监控
	rabbitmqQueueLength = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "rabbitmq_queue_length",
			Help: "Number of messages in RabbitMQ queues",
		},
		[]string{"queue_name"}, // normal/priority/dlx
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
	
	// 任务重试次数监控
	taskRetryCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "transcode_task_retries_total",
			Help: "Total number of task retries",
		},
		[]string{"retry_count"},
	)
	
	durationHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "transcode_duration_seconds",
			Help:    "Duration of transcode tasks in seconds",
			Buckets: []float64{10, 20, 40, 80, 160, 320, 640, 1280},
		},
	)
)

func init() {
	// 合并注册，避免重复
	prometheus.MustRegister(
		taskCounter,
		durationHist,
		nodeTaskCounter,
		rabbitmqQueueLength,
		nodeCPUUsage,
		nodeMemoryUsage,
		taskRetryCounter,
	)
}

// UpdateRabbitMQMetrics 更新 RabbitMQ 队列长度指标
func UpdateRabbitMQMetrics() {
	queues := map[string]string{
		"normal":   queueName,
		"priority": priorityQueueName,
		"dlx":      dlxQueueName,
	}
	
	for label, queue := range queues {
		if length, err := GetQueueLength(queue); err == nil {
			rabbitmqQueueLength.WithLabelValues(label).Set(float64(length))
		}
	}
}

// UpdateNodeMetrics 更新节点系统资源指标
func UpdateNodeMetrics(nodeID string) {
	cpuPercent, memPercent, err := collectSystemMetrics()
	if err != nil {
		return
	}
	
	nodeCPUUsage.WithLabelValues(nodeID).Set(cpuPercent)
	nodeMemoryUsage.WithLabelValues(nodeID).Set(memPercent)
}

// UpdateTaskRetryMetrics 更新任务重试次数指标
func UpdateTaskRetryMetrics(retryCount int) {
	taskRetryCounter.WithLabelValues(fmt.Sprintf("%d", retryCount)).Inc()
}
