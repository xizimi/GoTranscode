package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// 初始化 Kafka（假设 Kafka 在 localhost:9092）
	InitKafka([]string{"127.0.0.1:9092"})

	// 启动消费者（处理转码任务）
	go StartConsumer()

	// 启动 HTTP API
	router := setupRouter()
	go func() {
		log.Println("API server starting on :8078")
		router.Run(":8078")
	}()

	// 启动 Prometheus 指标服务
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Prometheus metrics on :8081")
		http.ListenAndServe(":8081", nil)
	}()

	// 等待中断信号（Ctrl+C）
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// 优雅关闭 Kafka 连接
	kafkaWriter.Close()
	kafkaReader.Close()
	log.Println("Shutdown complete")
}