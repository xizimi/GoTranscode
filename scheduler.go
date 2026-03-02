// scheduler.go
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	// 命令行参数
	nodeID     = flag.String("node", "1", "Node ID for this instance")
	maxWorkers = flag.Int("workers", 4, "Number of concurrent workers")
	topic      = flag.String("topic", "transcode-jobs", "Kafka topic")
	groupID    = flag.String("group", "gotranscode-group-v1", "Consumer group ID")
	brokers    = flag.String("brokers", "localhost:9092", "Kafka brokers (comma separated)")
	
	// 全局变量
	kafkaWriter *kafka.Writer
	kafkaReader *kafka.Reader
	jobQueue    chan Job
	
	jobStatusMap = make(map[string]*JobStatus)
	statusMu     sync.RWMutex
)

type Job struct {
	InputPath      string   `json:"input_path"`
	OutputProfiles []string `json:"output_profiles"`
	JobID          string   `json:"job_id,omitempty"`
	NodeID         string   `json:"node_id,omitempty"` // 记录处理节点
}

type JobStatus struct {
	JobID      string    `json:"job_id"`
	InputPath  string    `json:"input_path"`
	NodeID     string    `json:"node_id"`      // 哪个节点处理的
	Status     string    `json:"status"`       // pending/running/completed/failed
	Progress   float64   `json:"progress"`
	StartTime  time.Time `json:"start_time"`
	EndTime    time.Time `json:"end_time,omitempty"`
	ErrorMsg   string    `json:"error_msg,omitempty"`
}

func init() {
	flag.Parse()
	
	// 初始化队列
	jobQueue = make(chan Job, *maxWorkers*2)
	
	// 初始化 Kafka
}

func generateJobID(inputPath string) string {
	timestamp := time.Now().UnixNano()
	random := make([]byte, 4)
	rand.Read(random)
	return fmt.Sprintf("%s_%d_%s", 
		*nodeID,           // 加上节点标识，方便追踪
		timestamp, 
		hex.EncodeToString(random),
	)
}

// // 删除原来的 initKafka 函数，替换为：
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

func AddJob(job Job) error {
	if job.JobID == "" {
		job.JobID = generateJobID(job.InputPath)
	}
	
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	
	// 使用 JobID 作为 key，确保相同任务路由到同一 partition（顺序处理）
	return kafkaWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(job.JobID),
		Value: jobBytes,
	})
}

func updateJobStatus(jobID, nodeID, status string, progress float64, errMsg string) {
	statusMu.Lock()
	defer statusMu.Unlock()
	
	if s, ok := jobStatusMap[jobID]; ok {
		s.Status = status
		s.Progress = progress
		s.NodeID = nodeID
		if errMsg != "" {
			s.ErrorMsg = errMsg
		}
		if status == "completed" || status == "failed" {
			s.EndTime = time.Now()
		}
	} else {
		jobStatusMap[jobID] = &JobStatus{
			JobID:     jobID,
			NodeID:    nodeID,
			Status:    status,
			Progress:  progress,
			StartTime: time.Now(),
		}
	}
}

// GetAllStatus 返回所有节点状态（分布式调试用）
func GetAllStatus() map[string]*JobStatus {
	statusMu.RLock()
	defer statusMu.RUnlock()
	
	// 深拷贝
	result := make(map[string]*JobStatus)
	for k, v := range jobStatusMap {
		copy := *v
		result[k] = &copy
	}
	return result
}

func worker(id int) {
	for job := range jobQueue {
		jobID := job.JobID
		if jobID == "" {
			jobID = generateJobID(job.InputPath)
		}
		
		// 标记本节点处理
		job.NodeID = *nodeID
		updateJobStatus(jobID, *nodeID, "running", 0, "")
		
		log.Printf("[Node %s][Worker %d] Start: %s (jobID: %s)", 
			*nodeID, id, job.InputPath, jobID)
		
		start := time.Now()
		if err := Transcode(job.InputPath, job.OutputProfiles, jobID); err != nil {
			log.Printf("[Node %s][Worker %d] Failed: %s, error: %v", 
				*nodeID, id, job.InputPath, err)
			updateJobStatus(jobID, *nodeID, "failed", 0, err.Error())
		} else {
			duration := time.Since(start)
			log.Printf("[Node %s][Worker %d] Completed: %s in %v", 
				*nodeID, id, job.InputPath, duration)
			updateJobStatus(jobID, *nodeID, "completed", 100, "")
		}
	}
}

func StartConsumer() {
	log.Printf("Starting Node %s with %d workers, topic: %s, group: %s", 
		*nodeID, *maxWorkers, *topic, *groupID)
	
	// 启动 workers
	for i := 0; i < *maxWorkers; i++ {
		go worker(i)
	}
	
	// 优雅退出
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	
	go func() {
		sig := <-sigchan
		log.Printf("[Node %s] Received signal: %v, shutting down...", *nodeID, sig)
		cancel()
		close(jobQueue)
	}()
	
	// 消费循环 - 阻塞模式，无超时
	for {
		select {
		case <-ctx.Done():
			log.Printf("[Node %s] Consumer loop exiting", *nodeID)
			return
		default:
		}
		
		// 阻塞读取，直到有消息或 ctx 被取消
		msg, err := kafkaReader.ReadMessage(ctx)
		
		if err != nil {
			// 只有 ctx 被取消时才退出，其他错误重试
			if err == context.Canceled {
				log.Printf("[Node %s] Context canceled, exiting", *nodeID)
				return
			}
			
			// 真实错误（连接断开等），打印并重试
			log.Printf("[Node %s] Kafka error: %v", *nodeID, err)
			time.Sleep(time.Second)
			continue
		}

		var job Job
		if err := json.Unmarshal(msg.Value, &job); err != nil {
			log.Printf("[Node %s] Invalid job JSON: %s", *nodeID, string(msg.Value))
			continue
		}

		if job.JobID == "" {
			job.JobID = generateJobID(job.InputPath)
		}
		
		updateJobStatus(job.JobID, *nodeID, "pending", 0, "")

		select {
		case jobQueue <- job:
			log.Printf("[Node %s] Job queued: %s (jobID: %s)", 
				*nodeID, job.InputPath, job.JobID)
		case <-ctx.Done():
			log.Printf("[Node %s] Shutdown during job enqueue", *nodeID)
			return
		}
	}
}