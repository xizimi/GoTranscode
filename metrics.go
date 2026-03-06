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
	
	// RabbitMQ 队列长度监控 - 按队列组分类
	rabbitmqQueueLength = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "rabbitmq_queue_length",
			Help: "Number of messages in RabbitMQ queues",
		},
		[]string{"queue_group", "queue_name"}, // queue_group: normal/priority, queue_name: main/dlx
	)
	
	// 节点按队列组的任务统计
	nodeQueueGroupTaskCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "node_queue_group_tasks_total",
			Help: "Number of tasks processed by node and queue group",
		},
		[]string{"node_id", "queue_group", "status"},
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
		nodeQueueGroupTaskCounter,
	)
}

// UpdateRabbitMQMetrics 更新 RabbitMQ 队列长度指标
func UpdateRabbitMQMetrics() {
	queues := map[string]map[string]string{
		"normal": {
			"main": queueName,
			"dlx":  dlxQueueName,
		},
		"priority": {
			"main": priorityQueueName,
			"dlx":  priorityDlxQueueName,
		},
	}
	
	for group, queueMap := range queues {
		for queueType, queue := range queueMap {
			if length, err := GetQueueLength(queue); err == nil {
				rabbitmqQueueLength.WithLabelValues(group, queueType).Set(float64(length))
			}
		}
	}
}

// UpdateNodeQueueGroupMetrics 更新节点按队列组的任务指标
// 参数：nodeID-节点 ID, queueGroup-队列组 (normal/priority), status-任务状态 (completed/failed)
func UpdateNodeQueueGroupMetrics(nodeID, queueGroup, status string) {
	nodeQueueGroupTaskCounter.WithLabelValues(nodeID, queueGroup, status).Inc()
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
