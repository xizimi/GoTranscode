package main


// // 全局 Kafka 客户端（简单起见，实际项目建议封装）
// var (
// 	kafkaWriter *kafka.Writer
// 	kafkaReader *kafka.Reader
// )

type Job struct {
	InputPath      string   `json:"input_path"`
	OutputProfiles []string `json:"output_profiles"`
}