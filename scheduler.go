package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	// 命令行参数定义 (保持指针类型)
	// nodeIDFlag     = flag.String("node", "1", "Node ID for this instance")
	maxWorkers     = flag.Int("workers", 4, "Number of concurrent workers")
	topic          = flag.String("topic", "transcode-jobs", "Kafka topic")
	groupID        = flag.String("group", "gotranscode-group-v1", "Consumer group ID")

	// 【关键】不再在 init 中赋值，移到 main()
	currentNodeID string

	// 全局变量
	kafkaWriter *kafka.Writer
	kafkaReader *kafka.Reader
	jobQueue    chan Job

	// 本地负载计数器 + 关闭控制
	localLoadCount int
	loadMu         sync.Mutex
	closeOnce      sync.Once
	shutdownChan   chan struct{}
)

type Job struct {
	InputPath      string   `json:"input_path"`
	OutputProfiles []string `json:"output_profiles"`
	JobID          string   `json:"job_id,omitempty"`
	NodeID         string   `json:"node_id,omitempty"`
}

type JobStatus struct {
	JobID      string    `json:"job_id"`
	InputPath  string    `json:"input_path"`
	NodeID     string    `json:"node_id"`
	Status     string    `json:"status"` // pending/running/completed/failed
	Progress   float64   `json:"progress"`
	StartTime  time.Time `json:"start_time"`
	EndTime    time.Time `json:"end_time,omitempty"`
	ErrorMsg   string    `json:"error_msg,omitempty"`
}

func generateJobID(inputPath string) string {
	timestamp := time.Now().UnixNano()
	random := make([]byte, 4)
	rand.Read(random)
	return fmt.Sprintf("%s_%d_%s",
		currentNodeID,
		timestamp,
		hex.EncodeToString(random),
	)
}

func InitKafka(brokerList []string) {
	transport := &kafka.Transport{
		DialTimeout: 10 * time.Second,
	}

	kafkaWriter = &kafka.Writer{
		Addr:      kafka.TCP(brokerList...),
		Topic:     *topic,
		Balancer:  &kafka.LeastBytes{},
		Transport: transport,
	}

	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokerList,
		Topic:          *topic,
		GroupID:        *groupID,
		CommitInterval: 5 * time.Second,
		Dialer: &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: false,
		},
	})
}

func AddJob(job Job) (string, error) {
	if job.JobID == "" {
		job.JobID = generateJobID(job.InputPath)
	}

	jobBytes, err := json.Marshal(job)
	if err != nil {
		return "", fmt.Errorf("marshal job: %w", err)
	}

	err = kafkaWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(job.JobID),
		Value: jobBytes,
	})
	if err != nil {
		return "", fmt.Errorf("write kafka message: %w", err)
	}

	return job.JobID, nil
}

// updateJobStatus 重写：写入 Redis
func updateJobStatus(jobID, nID, status string, progress float64, errMsg string) {
	var startTime time.Time
	var inputPath string

	oldStatus, _ := GetJobStatusFromRedis(jobID)
	if oldStatus != nil {
		startTime = oldStatus.StartTime
		inputPath = oldStatus.InputPath
	} else {
		startTime = time.Now()
	}

	js := &JobStatus{
		JobID:     jobID,
		NodeID:    nID,
		Status:    status,
		Progress:  progress,
		ErrorMsg:  errMsg,
		StartTime: startTime,
		InputPath: inputPath,
	}

	if status == "completed" || status == "failed" {
		js.EndTime = time.Now()
	}

	if err := UpdateJobStatusInRedis(js); err != nil {
		log.Printf("[Node %s] Failed to update job status in Redis: %v", nID, err)
	}
}

// startHeartbeat 定期向 Redis 汇报节点状态
func startHeartbeat() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		loadMu.Lock()
		currentLoad := localLoadCount
		loadMu.Unlock()

		if err := ReportNodeHeartbeat(currentNodeID, currentLoad); err != nil {
			log.Printf("[Node %s] Heartbeat failed: %v", currentNodeID, err)
		}
	}
}

func worker(id int, shutdown <-chan struct{}) {
	for {
		select {
		case <-shutdown:
			log.Printf("[Node %s][Worker %d] Shutting down", currentNodeID, id)
			return
		case job, ok := <-jobQueue:
			if !ok {
				log.Printf("[Node %s][Worker %d] JobQueue closed, exiting", currentNodeID, id)
				return
			}

			jobID := job.JobID
			if jobID == "" {
				jobID = generateJobID(job.InputPath)
			}

			job.NodeID = currentNodeID

			loadMu.Lock()
			localLoadCount++
			loadMu.Unlock()

			AddJobToNode(currentNodeID, jobID)
			updateJobStatus(jobID, currentNodeID, "running", 0, "")

			log.Printf("[Node %s][Worker %d] Start: %s (jobID: %s)",
				currentNodeID, id, job.InputPath, jobID)

			start := time.Now()
			err := Transcode(job.InputPath, job.OutputProfiles, jobID)

			duration := time.Since(start).Seconds()

			if err != nil {
				log.Printf("[Node %s][Worker %d] Failed: %s, error: %v",
					currentNodeID, id, job.InputPath, err)
				updateJobStatus(jobID, currentNodeID, "failed", 0, err.Error())
				taskCounter.WithLabelValues("failed").Inc()
			} else {
				log.Printf("[Node %s][Worker %d] Completed: %s in %v",
					currentNodeID, id, job.InputPath, time.Duration(duration*1e9))
				updateJobStatus(jobID, currentNodeID, "completed", 100, "")
				taskCounter.WithLabelValues("completed").Inc()
				durationHist.Observe(duration)
			}

			RemoveJobFromNode(currentNodeID, jobID)

			loadMu.Lock()
			localLoadCount--
			loadMu.Unlock()
		}
	}
}

func StartConsumer(shutdown <-chan struct{}) {
	log.Printf("Starting Node %s with %d workers, topic: %s, group: %s",
		currentNodeID, *maxWorkers, *topic, *groupID)

	go startHeartbeat()

	for i := 0; i < *maxWorkers; i++ {
		go worker(i, shutdown)
	}

	for {
		select {
		case <-shutdown:
			log.Printf("[Node %s] Shutdown signal received, stopping consumer", currentNodeID)
			return
		default:
			msg, err := kafkaReader.ReadMessage(context.Background())

			if err != nil {
				if err == context.Canceled {
					log.Printf("[Node %s] Context canceled, exiting", currentNodeID)
					return
				}
				log.Printf("[Node %s] Kafka error: %v", currentNodeID, err)
				time.Sleep(time.Second)
				continue
			}

			var job Job
			if err := json.Unmarshal(msg.Value, &job); err != nil {
				log.Printf("[Node %s] Invalid job JSON: %s", currentNodeID, string(msg.Value))
				continue
			}

			if job.JobID == "" {
				job.JobID = generateJobID(job.InputPath)
			}

			updateJobStatus(job.JobID, currentNodeID, "pending", 0, "")

			select {
			case jobQueue <- job:
				log.Printf("[Node %s] Job queued: %s (jobID: %s)",
					currentNodeID, job.InputPath, job.JobID)
			case <-shutdown:
				return
			}
		}
	}
}

// GracefulShutdown 优雅关闭所有资源
func GracefulShutdown() {
	closeOnce.Do(func() {
		close(shutdownChan)
		
		// 等待 5 秒让 worker 处理完当前任务
		time.Sleep(2 * time.Second)
		
		// 关闭 jobQueue
		close(jobQueue)
		
		// 关闭 Kafka 连接
		if kafkaWriter != nil {
			kafkaWriter.Close()
		}
		if kafkaReader != nil {
			kafkaReader.Close()
		}
		
		// 关闭 Redis
		if rdb != nil {
			rdb.Close()
		}
		
		log.Println("Graceful shutdown complete")
	})
}
