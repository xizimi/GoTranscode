// transcoder.go
package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
)

func getResolution(profile string) string {
	switch profile {
	case "360p":
		return "640:360"
	case "480p":
		return "854:480"
	case "720p":
		return "1280:720"
	case "1080p":
		return "1920:1080"
	default:
		return "1280:720" // silent fallback
	}
}

func Transcode(inputURL string, profiles []string, jobID string) error {
	absInput, err := filepath.Abs(inputURL)
	if err != nil {
		return fmt.Errorf("resolve path: %w", err)
	}
	if _, err := os.Stat(absInput); os.IsNotExist(err) {
		return fmt.Errorf("input not found: %s", absInput)
	}

	outputDir := "./output_path"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	for _, profile := range profiles {
		res := getResolution(profile)
		outputPath := filepath.Join(outputDir, fmt.Sprintf("%s_%s.m3u8", jobID, profile))

		cmd := exec.Command("ffmpeg",
			"-i", absInput,
			"-vf", "scale="+res,
			"-c:v", "libx264",
			"-crf", "23",
			"-preset", "fast",
			"-c:a", "aac",
			"-b:a", "128k",
			"-ac", "2",
			"-hls_time", "4",
			"-hls_list_size", "0",
			"-f", "hls",
			outputPath,
		)

		stderr, err := cmd.StderrPipe()
		if err != nil {
			return fmt.Errorf("stderr pipe: %w", err)
		}

		if err := cmd.Start(); err != nil {
			return fmt.Errorf("start ffmpeg: %w", err)
		}

		// 读取 stderr（不打印，但可用于日志系统 future-proof）
		errMsg, _ := io.ReadAll(stderr)
		if waitErr := cmd.Wait(); waitErr != nil {
			// 不再打印到控制台，只返回错误
			return fmt.Errorf("ffmpeg failed for profile %s: %s", profile, string(errMsg))
		}
	}
	return nil
}