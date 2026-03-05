package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

type Job struct {
	InputPath      string   `json:"input_path"`
	OutputProfiles []string `json:"output_profiles"`
	JobID          string   `json:"job_id,omitempty"`
	NodeID         string   `json:"node_id,omitempty"`
	IsVIP          bool     `json:"is_vip,omitempty"` // 新增：VIP 任务标识
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
	RetryCount int       `json:"retry_count,omitempty"` // 新增：重试次数
}

func generateJobID(inputPath string) string {
	timestamp := time.Now().UnixNano()
	random := make([]byte, 4)
	_, err := rand.Read(random)
	if err != nil {
		// fallback to timestamp only
		return fmt.Sprintf("%s_%d", currentNodeID, timestamp)
	}
	return fmt.Sprintf("%s_%d_%s",
		currentNodeID,
		timestamp,
		hex.EncodeToString(random),
	)
}

// updateJobStatus 更新任务状态到 Redis
func updateJobStatus(jobID, nID, status string, progress float64, errMsg string) {
	var startTime time.Time
	var inputPath string
	var retryCount int

	oldStatus, _ := GetJobStatusFromRedis(jobID)
	if oldStatus != nil {
		startTime = oldStatus.StartTime
		inputPath = oldStatus.InputPath
		retryCount = oldStatus.RetryCount
	} else {
		startTime = time.Now()
		// 尝试从 jobRetryInfo 获取 inputPath（如果存在）
		if info, ok := jobRetryInfo.Load(jobID); ok {
			if rInfo, ok := info.(*RetryInfo); ok {
				var jobMsg JobMessage
				if err := json.Unmarshal(rInfo.Body, &jobMsg); err == nil {
					inputPath = jobMsg.InputPath
				}
			}
		}
	}

	js := &JobStatus{
		JobID:      jobID,
		NodeID:     nID,
		Status:     status,
		Progress:   progress,
		ErrorMsg:   errMsg,
		StartTime:  startTime,
		InputPath:  inputPath,
		RetryCount: retryCount,
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

// 采集系统资源使用率
func collectSystemMetrics() (cpuPercent float64, memPercent float64, err error) {
	// 获取 CPU 使用率
	cpuPercentages, err := cpu.Percent(time.Second, false)
	if err != nil {
		return 0, 0, err
	}
	if len(cpuPercentages) > 0 {
		cpuPercent = cpuPercentages[0]
	}
	
	// 获取内存使用率
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return cpuPercent, 0, err
	}
	memPercent = memInfo.UsedPercent
	
	return cpuPercent, memPercent, nil
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

			// 设置 NodeID（唯一设置点）
			job.NodeID = currentNodeID

			loadMu.Lock()
			localLoadCount++
			loadMu.Unlock()

			AddJobToNode(currentNodeID, jobID)
			
			// 获取重试次数
			retryCount := 0
			isRetry := false
			if info, ok := jobRetryInfo.Load(jobID); ok {
				if rInfo, ok := info.(*RetryInfo); ok {
					retryCount = rInfo.RetryCount
					if retryCount > 0 {
						isRetry = true
					}
				}
			}
			updateJobStatus(jobID, currentNodeID, "running", 0, "")

			vipTag := ""
			if job.IsVIP {
				vipTag = "[VIP] "
			}
			log.Printf("[Node %s][Worker %d] %sStart: %s (jobID: %s, retry: %d)",
				currentNodeID, id, vipTag, job.InputPath, jobID, retryCount)

			// 修复：如果是重试任务，更新重试指标
			if isRetry {
				UpdateTaskRetryMetrics(retryCount)
			}

			start := time.Now()
			err := Transcode(job.InputPath, job.OutputProfiles, jobID)

			duration := time.Since(start).Seconds()

			if err != nil {
				log.Printf("[Node %s][Worker %d] Failed: %s, error: %v",
					currentNodeID, id, job.InputPath, err)
				
				// 处理失败：触发死信队列重试
				if info, ok := jobRetryInfo.Load(jobID); ok {
					if rInfo, ok := info.(*RetryInfo); ok {
						if requeueErr := RequeueFailedJob(jobID, rInfo.Body, rInfo.RetryCount); requeueErr != nil {
							log.Printf("[Node %s] Failed to requeue job %s: %v", currentNodeID, jobID, requeueErr)
						}
					}
				}
				// 修复：延迟删除，确保 RequeueFailedJob 能获取到重试信息
				
				updateJobStatus(jobID, currentNodeID, "failed", 0, err.Error())
				taskCounter.WithLabelValues("failed").Inc()
				statsMu.Lock()
				nodeFailedCount++
				nodeTaskCounter.WithLabelValues(currentNodeID, "failed").Inc()
				statsMu.Unlock()
			} else {
				log.Printf("[Node %s][Worker %d] Completed: %s in %v",
					currentNodeID, id, job.InputPath, time.Duration(duration*1e9))
				jobRetryInfo.Delete(jobID)
				updateJobStatus(jobID, currentNodeID, "completed", 100, "")
				taskCounter.WithLabelValues("completed").Inc()
				statsMu.Lock()
				nodeCompletedCount++
				nodeTaskCounter.WithLabelValues(currentNodeID, "completed").Inc()
				statsMu.Unlock()
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
	log.Printf("Starting Node %s with %d workers", currentNodeID, *maxWorkers)

	go startHeartbeat()

	// 初始化 RabbitMQ 消费者
	if err := ConsumeJobs(shutdown, jobQueue); err != nil {
		log.Fatalf("[Node %s] Failed to start RabbitMQ consumer: %v", currentNodeID, err)
	}

	for i := 0; i < *maxWorkers; i++ {
		go worker(i, shutdown)
	}

	// 等待关闭信号
	<-shutdown
	log.Printf("[Node %s] Consumer stopped", currentNodeID)
}

// GracefulShutdown 优雅关闭所有资源
func GracefulShutdown() {
	closeOnce.Do(func() {
		close(shutdownChan)
		
		// 等待 worker 处理完当前任务
		time.Sleep(2 * time.Second)
		
		// 关闭 jobQueue
		close(jobQueue)
		
		// 关闭 RabbitMQ 连接
		if err := CloseRabbitMQ(); err != nil {
			log.Printf("Error closing RabbitMQ: %v", err)
		}
		
		// 关闭 Redis
		if rdb != nil {
			rdb.Close()
		}
		
		log.Println("Graceful shutdown complete")
	})
}