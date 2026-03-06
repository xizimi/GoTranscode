package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	rdb *redis.Client
	ctx = context.Background()

	keyJobPrefix      = "transcode:job:"
	keyNodePrefix     = "transcode:node:"
	keyNodeJobsPrefix = "transcode:node:jobs:"
	keyAllNodesSet    = "transcode:nodes:active"
)

func InitRedis(addr, password string, db int) error {
	rdb = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to connect to redis: %w", err)
	}
	log.Println("Connected to Redis successfully")
	return nil
}

func UpdateJobStatusInRedis(status *JobStatus) error {
	key := keyJobPrefix + status.JobID

	data, err := json.Marshal(status)
	if err != nil {
		return err
	}

	pipe := rdb.Pipeline()
	pipe.HSet(ctx, key, "data", string(data))
	pipe.HSet(ctx, key, "status", status.Status)
	pipe.HSet(ctx, key, "node_id", status.NodeID)
	pipe.HSet(ctx, key, "progress", fmt.Sprintf("%f", status.Progress))
	pipe.HSet(ctx, key, "updated_at", fmt.Sprintf("%d", time.Now().Unix()))

	if status.Status == "completed" || status.Status == "failed" {
		pipe.Expire(ctx, key, 24*time.Hour)
	}

	_, err = pipe.Exec(ctx)
	return err
}

func GetJobStatusFromRedis(jobID string) (*JobStatus, error) {
	key := keyJobPrefix + jobID
	val, err := rdb.HGet(ctx, key, "data").Result()

	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var status JobStatus
	if err := json.Unmarshal([]byte(val), &status); err != nil {
		return nil, err
	}
	return &status, nil
}

func ReportNodeHeartbeat(nodeID string, currentLoad int) error {
	nodeKey := keyNodePrefix + nodeID
	
	// 获取节点任务统计
	statsMu.Lock()
	completedCount := nodeCompletedCount
	failedCount := nodeFailedCount
	statsMu.Unlock()
	
	// 采集系统资源（用于 Redis 存储，不打印日志）
	cpuPercent, memPercent, _ := collectSystemMetrics()

	// 获取各队列组当前负载
	normalMainLen, _ := GetQueueLength(queueName)
	normalDlxLen, _ := GetQueueLength(dlxQueueName)
	priorityMainLen, _ := GetQueueLength(priorityQueueName)
	priorityDlxLen, _ := GetQueueLength(priorityDlxQueueName)

	info := map[string]interface{}{
		"last_heartbeat":        time.Now().Format(time.RFC3339),
		"load":                  currentLoad,
		"status":                "online",
		"completed_tasks":       completedCount,
		"failed_tasks":          failedCount,
		"cpu_usage_percent":     cpuPercent,
		"memory_usage_percent":  memPercent,
		// 修复：队列组监控信息，支持按组隔离监控
		"queue_group_lengths": map[string]map[string]int64{
			"normal": {
				"main": normalMainLen,
				"dlx":  normalDlxLen,
			},
			"priority": {
				"main": priorityMainLen,
				"dlx":  priorityDlxLen,
			},
		},
		// 新增：节点 ID 用于区分不同节点
		"node_id": nodeID,
		// 新增：节点分组信息
		"node_group":            currentNodeGroup,
	}

	data, _ := json.Marshal(info)

	pipe := rdb.Pipeline()
	pipe.Set(ctx, nodeKey, string(data), 0)
	pipe.SAdd(ctx, keyAllNodesSet, nodeID)
	pipe.Expire(ctx, nodeKey, 30*time.Second)

	_, err := pipe.Exec(ctx)
	return err
}

func AddJobToNode(nodeID, jobID string) error {
	return rdb.SAdd(ctx, keyNodeJobsPrefix+nodeID, jobID).Err()
}

func RemoveJobFromNode(nodeID, jobID string) error {
	return rdb.SRem(ctx, keyNodeJobsPrefix+nodeID, jobID).Err()
}

func GetRunningJobsCountForNode(nodeID string) (int64, error) {
	return rdb.SCard(ctx, keyNodeJobsPrefix+nodeID).Result()
}

func GetAllActiveNodes() ([]map[string]interface{}, error) {
	nodes, err := rdb.SMembers(ctx, keyAllNodesSet).Result()
	if err != nil {
		return nil, err
	}

	activeNodes := make([]map[string]interface{}, 0)

	for _, nodeID := range nodes {
		val, err := rdb.Get(ctx, keyNodePrefix+nodeID).Result()
		if err == redis.Nil {
			rdb.SRem(ctx, keyAllNodesSet, nodeID)
			continue
		}
		if err != nil {
			log.Printf("Error getting node %s info: %v", nodeID, err)
			continue
		}

		var info map[string]interface{}
		if err := json.Unmarshal([]byte(val), &info); err != nil {
			continue
		}

		count, _ := GetRunningJobsCountForNode(nodeID)
		info["running_jobs_count"] = count
		info["node_id"] = nodeID

		activeNodes = append(activeNodes, info)
	}

	return activeNodes, nil
}