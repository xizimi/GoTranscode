// scheduler.go
package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const KAFKA_TOPIC = "transcode-jobs"

var (
	kafkaWriter *kafka.Writer
	kafkaReader *kafka.Reader
)

func InitKafka(brokers []string) {
	// 创建自定义传输层
	transport := &kafka.Transport{
		DialTimeout: 10 * time.Second,
		// 其他配置...
	}

	kafkaWriter = &kafka.Writer{
		Addr:      kafka.TCP(brokers...),
		Topic:     KAFKA_TOPIC,
		Balancer:  &kafka.LeastBytes{},
		Transport: transport, // ✅ 使用 Transport 替代 Dialer
	}

	// Reader 保持不变
	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   KAFKA_TOPIC,
		GroupID: "gotranscode-group",
		Dialer: &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: false,
		},
	})
}

func AddJob(job Job) error {
	jobBytes, _ := json.Marshal(job)
	return kafkaWriter.WriteMessages(context.Background(), kafka.Message{
		Value: jobBytes,
	})
}

func StartConsumer() {
	log.Println("Starting Kafka consumer for topic:", KAFKA_TOPIC)
	for {
		msg, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Kafka read error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var job Job
		if err := json.Unmarshal(msg.Value, &job); err != nil {
			log.Printf("Invalid job JSON: %s", string(msg.Value))
			continue
		}

		log.Printf("Processing job: %s", job.InputPath)
		go func(j Job) {
			if err := Transcode(j.InputPath, j.OutputProfiles, ""); err != nil {
				log.Printf("Transcode failed for %s: %v", j.InputPath, err)
			} else {
				log.Printf("Transcode completed for %s", j.InputPath)
			}
		}(job)
	}
}