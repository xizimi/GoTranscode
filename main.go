// main.go
package main

import (
	"os/exec"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// 初始化输出目录
	exec.Command("mkdir", "-p", "./output_path").Run()

	r := gin.Default()

	// API 路由
	r.POST("/transcode", submitTranscode)
	r.GET("/jobs/:id", getJobStatus)

	 // Prometheus 指标（无需 net/http 显式导入）
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.Run(":8078")
}