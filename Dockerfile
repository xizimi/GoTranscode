FROM golang:1.21-alpine AS builder

# 安装系统依赖
RUN apk add --no-cache git

WORKDIR /app

# 复制go mod文件
COPY go.mod go.sum ./
RUN go mod download

# 复制源代码
COPY . .

# 构建二进制文件
RUN CGO_ENABLED=0 GOOS=linux go build -o gotranscode .

FROM alpine:latest

# 安装FFmpeg
RUN apk add --no-cache ffmpeg ca-certificates

WORKDIR /app

# 从builder阶段复制二进制文件
COPY --from=builder /app/gotranscode .
COPY --from=builder /app/output_path ./output_path

# 创建日志目录
RUN mkdir -p /app/logs

EXPOSE 8078
EXPOSE 8066

# 启动命令
CMD ["./gotranscode"]