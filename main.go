package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// 全局变量 - 统一在此定义
var (
	currentNodeID string
	jobQueue      chan Job
	shutdownChan  chan struct{}
	closeOnce     sync.Once
	
	// 负载统计
	loadMu          sync.Mutex
	localLoadCount  int
	
	// 任务统计
	statsMu           sync.Mutex
	nodeCompletedCount int
	nodeFailedCount    int
)

// 全局 flags
var (
	nodeIDFlag    = flag.String("node-id", "node-1", "Unique ID for this transcoding node")
	nodeGroup     = flag.String("node-group", "all", "Node group: normal/priority/all") // 新增：节点分组
	rabbitAddr    = flag.String("rabbit-addr", "localhost:5672", "RabbitMQ server address")
	rabbitUser    = flag.String("rabbit-user", "guest", "RabbitMQ username")
	rabbitPass    = flag.String("rabbit-pass", "guest", "RabbitMQ password")
	redisAddr     = flag.String("redis-addr", "localhost:6379", "Redis server address")
	redisPassword = flag.String("redis-password", "", "Redis password")
	redisDB       = flag.Int("redis-db", 0, "Redis database number")
	enableAPI     = flag.Bool("enable-api", false, "Enable HTTP API server (only one node should set this to true)")
	maxWorkers    = flag.Int("workers", 4, "Number of concurrent workers")
)

func setupRouter() *gin.Engine {
	r := gin.Default()
	r.POST("/transcode", transcodeHandler)
	r.POST("/transcode/vip", transcodeVIPHandler)
	r.GET("/health", healthHandler)
	r.GET("/cluster/status", clusterStatusHandler)
	r.GET("/job/:id", jobStatusHandler)
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
		IsVIP:          false,
	}

	jobID, err := PublishJob(job, false)
	if err != nil {
		log.Printf("Failed to publish job: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to queue job"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Job queued successfully", "job_id": jobID})
}

func transcodeVIPHandler(c *gin.Context) {
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
		IsVIP:          true,
	}

	jobID, err := PublishJob(job, true)
	if err != nil {
		log.Printf("Failed to publish VIP job: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to queue VIP job"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "VIP job queued successfully", "job_id": jobID, "priority": "high"})
}

func healthHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "healthy"})
}

func clusterStatusHandler(c *gin.Context) {
	nodes, err := GetAllActiveNodes()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	// 获取 RabbitMQ 各队列组长度
	queueGroups := map[string]map[string]int64{
		"normal": {
			"main": 0,
			"dlx":  0,
		},
		"priority": {
			"main": 0,
			"dlx":  0,
		},
	}
	
	queueGroups["normal"]["main"], _ = GetQueueLength(queueName)
	queueGroups["normal"]["dlx"], _ = GetQueueLength(dlxQueueName)
	queueGroups["priority"]["main"], _ = GetQueueLength(priorityQueueName)
	queueGroups["priority"]["dlx"], _ = GetQueueLength(priorityDlxQueueName)
	
	// 实时采集系统指标
	cpuPercent, memPercent, _ := collectSystemMetrics()
	
	c.JSON(http.StatusOK, gin.H{
		"total_nodes":         len(nodes),
		"nodes":               nodes,
		"queue_groups":        queueGroups,
		"current_cpu_usage":   cpuPercent,
		"current_memory_usage": memPercent,
	})
}

func jobStatusHandler(c *gin.Context) {
	jobID := c.Param("id")
	status, err := GetJobStatusFromRedis(jobID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if status == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	c.JSON(http.StatusOK, status)
}

func main() {
	flag.Parse()

	currentNodeID = *nodeIDFlag
	
	// 验证节点分组参数
	if *nodeGroup != "normal" && *nodeGroup != "priority" && *nodeGroup != "all" {
		log.Fatalf("Invalid node-group: %s, must be normal/priority/all", *nodeGroup)
	}

	jobQueue = make(chan Job, *maxWorkers*2)
	log.Printf("Initialized Node: %s (group: %s) with jobQueue capacity: %d", currentNodeID, *nodeGroup, *maxWorkers*2)

	shutdownChan = make(chan struct{})

	// 1. 初始化 RabbitMQ
	if err := InitRabbitMQ(*rabbitAddr, *rabbitUser, *rabbitPass); err != nil {
		log.Fatalf("RabbitMQ init failed: %v", err)
	}

	// 2. 初始化 Redis
	if err := InitRedis(*redisAddr, *redisPassword, *redisDB); err != nil {
		log.Fatalf("Redis init failed: %v", err)
	}

	// 3. 启动消费者（所有节点都启动，但根据分组消费不同队列）
	go StartConsumer(shutdownChan)

	// 4. 仅当 enable-api=true 时启动 HTTP API 和 Metrics
	if *enableAPI {
		// 启动业务 API
		router := setupRouter()
		go func() {
			log.Println("API server starting on :8078")
			if err := router.Run(":8078"); err != nil {
				log.Fatalf("API server failed: %v", err)
			}
		}()

		// 启动 Prometheus metrics
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			log.Println("Prometheus metrics on :8066")
			if err := http.ListenAndServe(":8066", nil); err != nil {
				log.Fatalf("Metrics server failed: %v", err)
			}
		}()
	} else {
		log.Println("HTTP API and metrics disabled (worker-only mode)")
	}

	// 5. 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// 6. 优雅关闭
	log.Println("Shutting down...")
	GracefulShutdown()
}