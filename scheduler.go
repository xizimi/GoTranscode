// scheduler.go
package main

import (
	"sync"
	"time"
)

var (
	jobQueue   = make(chan *Job, 100)
	workerPool = make(chan struct{}, 3) // 最多3个并发
	mu         sync.RWMutex
)

func init() {
	for i := 0; i < 3; i++ {
		go worker()
	}
}

func EnqueueJob(job *Job) {
	jobQueue <- job
}

func worker() {
	for job := range jobQueue {
		// 获取 worker 许可
		workerPool <- struct{}{}
		processJob(job)
		<-workerPool // 释放
	}
}

func processJob(job *Job) {
	mu.Lock()
	job.Status = StatusProcessing
	jobStore[job.ID] = job
	mu.Unlock()

	startTime := time.Now()
	err := Transcode(job.InputURL, job.OutputProfiles, job.ID)
	duration := time.Since(startTime).Seconds()

	if err != nil {
		mu.Lock()
		job.Status = StatusFailed
		jobStore[job.ID] = job
		mu.Unlock()
		taskCounter.WithLabelValues("failed").Inc()
	} else {
		finishTime := time.Now()
		mu.Lock()
		job.Status = StatusCompleted
		job.FinishedAt = &finishTime
		// 假设输出路径按 profile 生成
		for _, p := range job.OutputProfiles {
			job.OutputPaths = append(job.OutputPaths, "./output_path/"+job.ID+"_"+p+".m3u8")
		}
		jobStore[job.ID] = job
		mu.Unlock()
		taskCounter.WithLabelValues("completed").Inc()
	}
	durationHist.Observe(duration)
}