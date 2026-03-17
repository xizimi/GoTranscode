package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	rabbitConn     *amqp.Connection
	rabbitChannel  *amqp.Channel
	rabbitMu       sync.Mutex
	
	// 队列名称 - 普通队列组
	queueName        = "transcode-jobs"
	dlxQueueName     = "transcode-jobs-dlx"
	dlxExchangeName  = "transcode-jobs-dlx-exchange"
	
	// 队列名称 - 优先队列组
	priorityQueueName      = "transcode-jobs-priority"
	priorityDlxQueueName   = "transcode-jobs-priority-dlx"
	priorityDlxExchangeName = "transcode-jobs-priority-dlx-exchange"
	
	// 最大重试次数
	maxRetryCount = 3
	
	// 节点分组（由 main.go 设置）
	currentNodeGroup string // normal/priority/all
)

// SetNodeGroup 设置当前节点分组
func SetNodeGroup(group string) {
	currentNodeGroup = group
}

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

	// 声明普通死信交换机
	err = rabbitChannel.ExchangeDeclare(
		dlxExchangeName,
		"direct", 
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare DLX exchange: %w", err)
	}

	// 声明普通死信队列
	_, err = rabbitChannel.QueueDeclare(
		dlxQueueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-message-ttl": int32(60000),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to declare DLX queue: %w", err)
	}

	// 绑定普通死信队列到死信交换机
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

	// 声明主队列（带普通死信配置）
	_, err = rabbitChannel.QueueDeclare(
		queueName,
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
		return fmt.Errorf("failed to declare main queue: %w", err)
	}

	// 声明优先死信交换机
	err = rabbitChannel.ExchangeDeclare(
		priorityDlxExchangeName,
		"direct", 
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare priority DLX exchange: %w", err)
	}

	// 声明优先死信队列
	_, err = rabbitChannel.QueueDeclare(
		priorityDlxQueueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-message-ttl": int32(60000),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to declare priority DLX queue: %w", err)
	}

	// 绑定优先死信队列到优先死信交换机
	err = rabbitChannel.QueueBind(
		priorityDlxQueueName,
		"",
		priorityDlxExchangeName,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind priority DLX queue: %w", err)
	}

	// 声明优先级队列（带优先死信配置）
	_, err = rabbitChannel.QueueDeclare(
		priorityQueueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange": priorityDlxExchangeName,
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
func RequeueFailedJob(jobID string, originalBody []byte, retryCount int, isVIP bool) error {
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

	// 根据任务类型选择回队队列
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
			Priority:     priority, // 修复：根据 isVIP 设置正确优先级
			MessageId:    jobID,
		},
	)
	rabbitMu.Unlock()
	if err != nil {
		return fmt.Errorf("requeue job: %w", err)
	}

	log.Printf("Job %s requeued (retry %d/%d, isVIP: %v)", jobID, msg.RetryCount, maxRetryCount, isVIP)
	return nil
}

// ConsumeJobs 消费转码任务（根据节点分组决定消费哪些队列）
func ConsumeJobs(shutdown <-chan struct{}, jobQueue chan<- Job) error {
	// 根据节点分组决定消费哪些队列
	consumeNormal := currentNodeGroup == "all" || currentNodeGroup == "normal"
	consumePriority := currentNodeGroup == "all" || currentNodeGroup == "priority"
	
	var normalMsgs, priorityMsgs, normalDlxMsgs, priorityDlxMsgs <-chan amqp.Delivery
	var err error
	
	// 消费主队列（普通任务）
	if consumeNormal {
		normalMsgs, err = rabbitChannel.Consume(
			queueName,
			fmt.Sprintf("consumer-%s", currentNodeID),
			false,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("failed to consume from main queue: %w", err)
		}
	}
	
	// 消费优先级队列（VIP 任务）
	if consumePriority {
		priorityMsgs, err = rabbitChannel.Consume(
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
	}
	
	// 消费普通死信队列
	if consumeNormal {
		normalDlxMsgs, err = rabbitChannel.Consume(
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
	}
	
	// 消费优先死信队列
	if consumePriority {
		priorityDlxMsgs, err = rabbitChannel.Consume(
			priorityDlxQueueName,
			fmt.Sprintf("priority-dlx-consumer-%s", currentNodeID),
			false,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("failed to consume from priority DLX queue: %w", err)
		}
	}
	
	// 根据分组启动对应的消费者协程
	if consumeNormal {
		go handleMessages(normalMsgs, shutdown, jobQueue, false, false)
		go handleDLXMessages(normalDlxMsgs, shutdown, jobQueue, false)
	}
	if consumePriority {
		go handleMessages(priorityMsgs, shutdown, jobQueue, true, false)
		go handleDLXMessages(priorityDlxMsgs, shutdown, jobQueue, true)
	}
	
	log.Printf("[Node %s][Group: %s] Started consuming queues: normal=%v, priority=%v", 
		currentNodeID, currentNodeGroup, consumeNormal, consumePriority)
	return nil
}

// handleMessages 处理普通和优先级队列消息
func handleMessages(msgs <-chan amqp.Delivery, shutdown <-chan struct{}, jobQueue chan<- Job, isPriority bool, isDLX bool) {
	queueType := "normal"
	if isPriority {
		queueType = "priority"
	}
	if isDLX {
		queueType += "-dlx"
	}
	
	for {
		select {
		case <-shutdown:
			log.Printf("[Node %s] %s consumer shutting down", currentNodeID, queueType)
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

			log.Printf("[Node %s] Job received from %s queue: %s", currentNodeID, queueType, jobID)

			updateJobStatus(jobID, currentNodeID, "pending", 0, "")

			// 创建完成通知通道
			completion := make(chan TaskCompletion, 1)

			select {
			case jobQueue <- jobMsg.Job:
				// 将原始消息体和完成通道存储，用于任务完成后 ACK/Nack
				jobRetryInfo.Store(jobID, &RetryInfo{
					Body:       msg.Body,
					RetryCount: jobMsg.RetryCount,
					IsVIP:      isPriority,
					Completion: completion, // 新增：存储完成通道
				})
				// 启动协程等待任务完成并处理 ACK/Nack
				go waitForTaskCompletion(msg, completion, jobID, isDLX)
			case <-shutdown:
				msg.Nack(false, false) // 不重新入队，避免重复消费
				return
			}
		}
	}
}

// TaskCompletion 任务完成通知
type TaskCompletion struct {
	Success bool
	JobID   string
}

// RetryInfo 存储重试信息
type RetryInfo struct {
	Body       []byte
	RetryCount int
	IsVIP      bool
	Completion chan TaskCompletion // 新增：任务完成通知通道
}

var jobRetryInfo sync.Map

// waitForTaskCompletion 等待任务完成并处理 ACK/Nack
func waitForTaskCompletion(msg amqp.Delivery, completion <-chan TaskCompletion, jobID string, isDLX bool) {
	select {
	case result := <-completion:
		if result.Success {
			msg.Ack(false)
			log.Printf("[Node %s] Job %s completed successfully, message ACKed", currentNodeID, jobID)
		} else {
			// 任务失败，Nack 并重新入队（进入死信队列）
			msg.Nack(false, true) // requeue=true，让消息进入死信队列
			log.Printf("[Node %s] Job %s failed, message Nacked to DLX", currentNodeID, jobID)
		}
	case <-time.After(30 * time.Minute): // 超时保护
		msg.Nack(false, false) // 超时不重新入队
		log.Printf("[Node %s] Job %s timed out, message Nacked without requeue", currentNodeID, jobID)
	}
}

// handleDLXMessages 处理死信队列消息（失败重试）
func handleDLXMessages(msgs <-chan amqp.Delivery, shutdown <-chan struct{}, jobQueue chan<- Job, isPriority bool) {
	queueType := "dlx"
	if isPriority {
		queueType = "priority-dlx"
	}
	
	for {
		select {
		case <-shutdown:
			log.Printf("[Node %s] %s consumer shutting down", currentNodeID, queueType)
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
			log.Printf("[Node %s] %s job received for retry: %s (attempt %d, isVIP: %v)", 
				currentNodeID, queueType, jobID, jobMsg.RetryCount, isPriority)

			updateJobStatus(jobID, currentNodeID, "pending", 0, "")

			// 创建完成通知通道
			completion := make(chan TaskCompletion, 1)

			select {
			case jobQueue <- jobMsg.Job:
				jobRetryInfo.Store(jobID, &RetryInfo{
					Body:       msg.Body,
					RetryCount: jobMsg.RetryCount,
					IsVIP:      isPriority,
					Completion: completion, // 新增：存储完成通道
				})
				// 启动协程等待任务完成并处理 ACK/Nack
				go waitForTaskCompletion(msg, completion, jobID, true)
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