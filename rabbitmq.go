package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	rabbitConn     *amqp.Connection
	rabbitChannel  *amqp.Channel
	rabbitMu       sync.Mutex
	
	// 队列名称
	queueName        = "transcode-jobs"
	priorityQueueName = "transcode-jobs-priority"
	dlxQueueName     = "transcode-jobs-dlx"
	dlxExchangeName  = "transcode-jobs-dlx-exchange"
	
	// 最大重试次数
	maxRetryCount = 3
)

// JobMessage 包含重试计数的消息包装
type JobMessage struct {
	Job
	RetryCount int `json:"retry_count"`
}

// InitRabbitMQ 初始化 RabbitMQ 连接和队列
func InitRabbitMQ(addr, user, password string) error {
	rabbitMu.Lock()
	defer rabbitMu.Unlock()

	// 构建连接 URL
	url := fmt.Sprintf("amqp://%s:%s@%s/", user, password, addr)
	
	var err error
	rabbitConn, err = amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	rabbitChannel, err = rabbitConn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}

	// 声明死信交换机
	err = rabbitChannel.ExchangeDeclare(
		dlxExchangeName,
		"direct",
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare DLX exchange: %w", err)
	}

	// 声明死信队列
	_, err = rabbitChannel.QueueDeclare(
		dlxQueueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-message-ttl": int32(60000), // 60 秒后重新入队
		},
	)
	if err != nil {
		return fmt.Errorf("failed to declare DLX queue: %w", err)
	}

	// 绑定死信队列到死信交换机
	err = rabbitChannel.QueueBind(
		dlxQueueName,
		"",
		dlxExchangeName,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind DLX queue: %w", err)
	}

	// 声明主队列（带死信配置）
	_, err = rabbitChannel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange": dlxExchangeName,
			"x-dead-letter-routing-key": "",
			"x-max-priority": int32(10), // 支持优先级 0-10
		},
	)
	if err != nil {
		return fmt.Errorf("failed to declare main queue: %w", err)
	}

	// 声明优先级队列（VIP 任务）
	_, err = rabbitChannel.QueueDeclare(
		priorityQueueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange": dlxExchangeName,
			"x-dead-letter-routing-key": "",
			"x-max-priority": int32(10),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to declare priority queue: %w", err)
	}

	log.Printf("Connected to RabbitMQ at %s", addr)
	return nil
}

// PublishJob 发布转码任务到队列
func PublishJob(job Job, isVIP bool) (string, error) {
	if job.JobID == "" {
		job.JobID = generateJobID(job.InputPath)
	}

	msg := JobMessage{
		Job:        job,
		RetryCount: 0,
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("marshal job: %w", err)
	}

	// 选择队列：VIP 任务使用优先级队列
	targetQueue := queueName
	priority := uint8(5) // 普通任务优先级
	if isVIP {
		targetQueue = priorityQueueName
		priority = 10 // VIP 任务最高优先级
	}

	rabbitMu.Lock()
	err = rabbitChannel.PublishWithContext(
		context.Background(),
		"",
		targetQueue,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
			Priority:     priority,
			MessageId:    job.JobID,
		},
	)
	rabbitMu.Unlock()
	if err != nil {
		return "", fmt.Errorf("publish job: %w", err)
	}

	log.Printf("Job published to %s: %s (VIP: %v)", targetQueue, job.JobID, isVIP)
	return job.JobID, nil
}

// RequeueFailedJob 将失败任务重新入队（死信队列处理）
func RequeueFailedJob(jobID string, originalBody []byte, retryCount int) error {
	if retryCount >= maxRetryCount {
		log.Printf("Job %s exceeded max retry count (%d), marking as failed", jobID, maxRetryCount)
		updateJobStatus(jobID, currentNodeID, "failed", 0, 
			fmt.Sprintf("max retry count (%d) exceeded", maxRetryCount))
		return nil
	}

	var msg JobMessage
	if err := json.Unmarshal(originalBody, &msg); err != nil {
		return fmt.Errorf("unmarshal job message: %w", err)
	}

	msg.RetryCount = retryCount + 1
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	rabbitMu.Lock()
	err = rabbitChannel.PublishWithContext(
		context.Background(),
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
			Priority:     5,
			MessageId:    jobID,
		},
	)
	rabbitMu.Unlock()
	if err != nil {
		return fmt.Errorf("requeue job: %w", err)
	}

	log.Printf("Job %s requeued (retry %d/%d)", jobID, msg.RetryCount, maxRetryCount)
	return nil
}

// ConsumeJobs 消费转码任务
func ConsumeJobs(shutdown <-chan struct{}, jobQueue chan<- Job) error {
	// 消费主队列
	msgs, err := rabbitChannel.Consume(
		queueName,
		fmt.Sprintf("consumer-%s", currentNodeID),
		false, // auto-ack = false, 手动确认
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to consume from main queue: %w", err)
	}

	// 消费优先级队列（VIP 任务）
	priorityMsgs, err := rabbitChannel.Consume(
		priorityQueueName,
		fmt.Sprintf("priority-consumer-%s", currentNodeID),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to consume from priority queue: %w", err)
	}

	// 消费死信队列
	dlxMsgs, err := rabbitChannel.Consume(
		dlxQueueName,
		fmt.Sprintf("dlx-consumer-%s", currentNodeID),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to consume from DLX queue: %w", err)
	}

	// 启动三个协程分别处理不同队列
	go handleMessages(msgs, shutdown, jobQueue, false)
	go handleMessages(priorityMsgs, shutdown, jobQueue, true)
	go handleDLXMessages(dlxMsgs, shutdown, jobQueue)

	log.Printf("Started consuming from queues: %s, %s, %s", queueName, priorityQueueName, dlxQueueName)
	return nil
}

// handleMessages 处理普通和优先级队列消息
func handleMessages(msgs <-chan amqp.Delivery, shutdown <-chan struct{}, jobQueue chan<- Job, isPriority bool) {
	for {
		select {
		case <-shutdown:
			log.Printf("[Node %s] Priority consumer shutting down", currentNodeID)
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}

			var jobMsg JobMessage
			if err := json.Unmarshal(msg.Body, &jobMsg); err != nil {
				log.Printf("[Node %s] Invalid job JSON: %s", currentNodeID, string(msg.Body))
				msg.Nack(false, false) // 不重新入队
				continue
			}

			jobID := jobMsg.JobID
			if jobID == "" {
				jobID = generateJobID(jobMsg.InputPath)
			}

			jobMsg.NodeID = currentNodeID

			queueType := "normal"
			if isPriority {
				queueType = "priority"
			}
			log.Printf("[Node %s] Job received from %s queue: %s", currentNodeID, queueType, jobID)

			updateJobStatus(jobID, currentNodeID, "pending", 0, "")

			select {
			case jobQueue <- jobMsg.Job:
				// 将原始消息体存储，用于失败时重试
				jobRetryInfo.Store(jobID, &RetryInfo{
					Body:       msg.Body,
					RetryCount: jobMsg.RetryCount,
				})
				msg.Ack(false)
			case <-shutdown:
				msg.Nack(false, false) // 不重新入队，避免重复消费
				return
			}
		}
	}
}

// RetryInfo 存储重试信息
type RetryInfo struct {
	Body       []byte
	RetryCount int
}

var jobRetryInfo sync.Map

// handleDLXMessages 处理死信队列消息（失败重试）
func handleDLXMessages(msgs <-chan amqp.Delivery, shutdown <-chan struct{}, jobQueue chan<- Job) {
	for {
		select {
		case <-shutdown:
			log.Printf("[Node %s] DLX consumer shutting down", currentNodeID)
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}

			var jobMsg JobMessage
			if err := json.Unmarshal(msg.Body, &jobMsg); err != nil {
				log.Printf("[Node %s] Invalid DLX job JSON: %s", currentNodeID, string(msg.Body))
				msg.Nack(false, false)
				continue
			}

			jobID := jobMsg.JobID
			log.Printf("[Node %s] DLX job received for retry: %s (attempt %d)", 
				currentNodeID, jobID, jobMsg.RetryCount)

			updateJobStatus(jobID, currentNodeID, "pending", 0, "")

			select {
			case jobQueue <- jobMsg.Job:
				jobRetryInfo.Store(jobID, &RetryInfo{
					Body:       msg.Body,
					RetryCount: jobMsg.RetryCount,
				})
				// 修复：重试指标在任务实际执行时更新（见 scheduler.go worker 函数）
				msg.Ack(false)
			case <-shutdown:
				msg.Nack(false, false)
				return
			}
		}
	}
}

// CloseRabbitMQ 关闭 RabbitMQ 连接
func CloseRabbitMQ() error {
	rabbitMu.Lock()
	defer rabbitMu.Unlock()

	if rabbitChannel != nil {
		if err := rabbitChannel.Close(); err != nil {
			log.Printf("Error closing RabbitMQ channel: %v", err)
		}
	}
	if rabbitConn != nil {
		if err := rabbitConn.Close(); err != nil {
			log.Printf("Error closing RabbitMQ connection: %v", err)
		}
	}

	log.Println("RabbitMQ connection closed")
	return nil
}

// GetQueueLength 获取队列长度（用于监控）
func GetQueueLength(queue string) (int64, error) {
	rabbitMu.Lock()
	defer rabbitMu.Unlock()
	
	queueInfo, err := rabbitChannel.QueueInspect(queue)
	if err != nil {
		return 0, err
	}
	return int64(queueInfo.Messages), nil
}