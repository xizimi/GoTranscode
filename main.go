package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// 全局 flags
var (
	nodeIDFlag    = flag.String("node-id", "node-1", "Unique ID for this transcoding node")
	brokers       = flag.String("kafka-brokers", "localhost:9092", "Kafka bootstrap brokers (comma-separated)")
	redisAddr     = flag.String("redis-addr", "localhost:6379", "Redis server address")
	enableAPI     = flag.Bool("enable-api", false, "Enable HTTP API server (only one node should set this to true)")
)



func setupRouter() *gin.Engine {
	r := gin.Default()
	r.POST("/transcode", transcodeHandler)
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
	}

	jobID, err := AddJob(job)
	if err != nil {
		log.Printf("Failed to add job: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to queue job"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Job queued successfully", "job_id": jobID})
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
	
	// 新增：获取 Kafka 队列长度
	var pendingTasks int64 = 0
	if kafkaReader != nil {
		stats := kafkaReader.Stats()
		pendingTasks = stats.Lag
	}
	
	// 新增：查询时实时采集系统指标（不再定时打印）
	cpuPercent, memPercent, _ := collectSystemMetrics()
	
	c.JSON(http.StatusOK, gin.H{
		"total_nodes":           len(nodes),
		"nodes":                 nodes,
		"kafka_pending_tasks":   pendingTasks,
		// 新增：实时系统指标（仅查询时返回）
		"current_cpu_usage":     cpuPercent,
		"current_memory_usage":  memPercent,
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

	jobQueue = make(chan Job, *maxWorkers*2)
	log.Printf("Initialized Node: %s with jobQueue capacity: %d", currentNodeID, *maxWorkers*2)

	shutdownChan = make(chan struct{})

	// 1. 初始化 Kafka
	InitKafka(strings.Split(*brokers, ","))

	// 2. 初始化 Redis
	if err := InitRedis(*redisAddr, "", 0); err != nil {
		log.Fatalf("Redis init failed: %v", err)
	}

	// 3. 启动消费者（所有节点都启动）
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