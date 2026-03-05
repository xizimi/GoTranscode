// transcoder.go
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

// Profile 定义分辨率档位及其优先级（越高越优先作为主档位）
var profilePriority = map[string]int{
	"1080p": 4,
	"720p":  3,
	"480p":  2,
	"360p":  1,
}

// FFmpeg 路径（启动时检测）
var ffmpegPath string

func init() {
	var err error
	ffmpegPath, err = exec.LookPath("ffmpeg")
	if err != nil {
		panic("ffmpeg not found in PATH: " + err.Error())
	}
}

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
		return "1280:720"
	}
}

// getBandwidth 获取档位对应的带宽值（用于 Master Playlist）
func getBandwidth(profile string) int {
	switch profile {
	case "360p":
		return 400000   // 实际 200-800k
	case "480p":
		return 800000   // 实际 400-1500k
	case "720p":
		return 1500000  // 实际 800-2500k
	case "1080p":
		return 3000000  // 实际 1500-4000k
	default:
		return 1500000
	}
}

// getAudioBitrate 获取档位对应的音频码率
func getAudioBitrate(profile string) string {
	switch profile {
	case "360p":
		return "64k"
	case "480p":
		return "96k"
	case "720p", "1080p":
		return "128k"
	default:
		return "128k"
	}
}

// getReferenceKB 获取档位的 CQP 探针参考大小（QP=23, 4秒）
func getReferenceKB(profile string) float64 {
	// QP=23, 4秒, 视频裸流大小
	switch profile {
	case "360p":
		return 150.0  // 300kbps × 4 / 8
	case "480p":
		return 250.0  // 500kbps × 4 / 8
	case "720p":
		return 400.0  // 800kbps × 4 / 8
	case "1080p":
		return 750.0  // 1500kbps × 4 / 8
	default:
		return 400.0
	}
}

// getHighestProfile 返回 profiles 中优先级最高的档位
func getHighestProfile(profiles []string) string {
	highest := ""
	maxPrio := 0
	for _, p := range profiles {
		if prio, ok := profilePriority[p]; ok && prio > maxPrio {
			maxPrio = prio
			highest = p
		}
	}
	if highest == "" && len(profiles) > 0 {
		return profiles[0] // fallback
	}
	return highest
}

// crfOffset 根据档位返回 CRF 偏移（低分辨率可容忍更高 CRF）
func crfOffset(profile string) int {
	switch profile {
	case "1080p", "720p":
		return 0
	case "480p":
		return 2
	case "360p":
		return 4
	default:
		return 2
	}
}

// crfFromComplexity 将复杂度映射到基础 CRF
// 简单内容允许更高压缩率(CRF↑)，复杂内容需要保质量(CRF↓)
// complexity: 0.5 (简单) → CRF=28 (高压缩)
//             2.0 (复杂) → CRF=18 (高质量)
func crfFromComplexity(complexity float64) int {
	// 线性映射: 0.5→28, 2.0→18
	crf := 28 - int((complexity-0.5)/1.5*10+0.5) // 范围扩大到 28-18
	if crf < 18 {
		return 18
	}
	if crf > 28 {
		return 28
	}
	return crf
}

// analyzeComplexityFromSize 基于 CQP 探针的文件大小估算复杂度（按档位）
func analyzeComplexityFromSize(tsPath, profile string) (complexity float64, err error) {
	info, err := os.Stat(tsPath)
	if err != nil {
		return 0, fmt.Errorf("stat probe TS: %w", err)
	}
	sizeKB := float64(info.Size()) / 1024.0
	referenceKB := getReferenceKB(profile)
	complexity = sizeKB / referenceKB
	if complexity < 0.5 {
		complexity = 0.5
	}
	if complexity > 2.0 {
		complexity = 2.0
	}
	return complexity, nil
}

// generateProbeTS 生成 CQP 探针（仅视频，用于分析）
func generateProbeTS(inputPath, profile, outputPath string) error {
	res := getResolution(profile)
	cmd := exec.Command(ffmpegPath,
		"-y",
		"-i", inputPath,
		"-t", "4",
		"-vf", "scale="+res,
		"-c:v", "libx264",
		"-preset", "ultrafast",
		"-qp", "23", // ← CQP 模式，固定 QP=23
		"-profile:v", "main",
		"-an", // ← 无音频，纯视频分析
		"-f", "mpegts",
		outputPath,
	)
	cmd.Stderr = io.Discard
	return cmd.Run()
}

// transcodeFull 转码单个档位（全量，含音频，带超时控制）
func transcodeFull(ctx context.Context, inputPath, profile, jobID string, crf int) error {
	// 使用绝对路径
	// 使用环境变量或默认当前目录下的 output_path
	outputDir := os.Getenv("TRANSCODE_OUTPUT_DIR")
	if outputDir == "" {
		outputDir = "./output_path"
	}
	absOutputDir, err := filepath.Abs(outputDir)
	if err != nil {
		return fmt.Errorf("resolve output dir: %w", err)
	}
	hlsTime := 4

	// 创建带超时的 context
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	segmentTemplate := filepath.Join(absOutputDir, fmt.Sprintf("%s_%s_seg_%%03d.ts", jobID, profile))
	finalM3U8 := filepath.Join(absOutputDir, fmt.Sprintf("%s_%s.m3u8", jobID, profile))

	audioBitrate := getAudioBitrate(profile)

	// 创建命令
	cmd := exec.CommandContext(ctx, ffmpegPath,
		"-y",
		"-i", inputPath,
		"-vf", "scale="+getResolution(profile),
		"-c:v", "libx264",
		"-crf", strconv.Itoa(crf),
		"-preset", "fast",
		"-force_key_frames", "expr:gte(t,n_forced*4)",
		"-sc_threshold", "0",
		"-profile:v", "main",
		"-c:a", "aac",
		"-b:a", audioBitrate,
		"-ac", "2",
		"-hls_time", strconv.Itoa(hlsTime),
		"-hls_list_size", "0",
		"-hls_segment_filename", segmentTemplate,
		"-f", "hls",
		finalM3U8,
	)

	// 执行命令并等待完成
	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("transcode timeout: %w", err)
		}
		return fmt.Errorf("ffmpeg failed: %w", err)
	}

	return nil
}

// generateMasterPlaylist 生成主播放列表（自适应码率切换）
func generateMasterPlaylist(profiles []string, jobID string) error {
	// 使用环境变量或默认当前目录下的 output_path
	outputDir := os.Getenv("TRANSCODE_OUTPUT_DIR")
	if outputDir == "" {
		outputDir = "./output_path"
	}
	absOutputDir, err := filepath.Abs(outputDir)
	if err != nil {
		return fmt.Errorf("resolve output dir: %w", err)
	}
	masterPath := filepath.Join(absOutputDir, fmt.Sprintf("%s.m3u8", jobID))

	file, err := os.Create(masterPath)
	if err != nil {
		return fmt.Errorf("create master playlist: %w", err)
	}
	
	defer file.Close()

	// 写入 HLS 头部
	file.WriteString("#EXTM3U\n")
	file.WriteString("#EXT-X-VERSION:3\n\n")

	// 按清晰度排序（从高到低）
	sortedProfiles := make([]string, len(profiles))
	copy(sortedProfiles, profiles)
	// 简单冒泡排序（按优先级降序）
	for i := 0; i < len(sortedProfiles); i++ {
		for j := i + 1; j < len(sortedProfiles); j++ {
			if profilePriority[sortedProfiles[i]] < profilePriority[sortedProfiles[j]] {
				sortedProfiles[i], sortedProfiles[j] = sortedProfiles[j], sortedProfiles[i]
			}
		}
	}

	// 写入每个档位
	for _, profile := range sortedProfiles {
		bandwidth := getBandwidth(profile)
		resolution := getResolution(profile)
		
		file.WriteString(fmt.Sprintf("#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%s\n",
			bandwidth, resolution))
		file.WriteString(fmt.Sprintf("%s_%s.m3u8\n\n", jobID, profile))
	}

	return nil
}

// Transcode 主入口：使用 CQP 探针，生成 Master Playlist
func Transcode(inputURL string, profiles []string, jobID string) error {
	if jobID == "" {
		return fmt.Errorf("jobID cannot be empty")
	}

	absInput, err := filepath.Abs(inputURL)
	if err != nil {
		return fmt.Errorf("resolve input: %w", err)
	}
	if _, err := os.Stat(absInput); os.IsNotExist(err) {
		return fmt.Errorf("input not found: %s", absInput)
	}

	if len(profiles) == 0 {
		return fmt.Errorf("profiles cannot be empty")
	}

	// 使用环境变量或默认当前目录下的 output_path
	outputDir := os.Getenv("TRANSCODE_OUTPUT_DIR")
	if outputDir == "" {
		outputDir = "./output_path"
	}
	absOutputDir, err := filepath.Abs(outputDir)
	if err != nil {
		return fmt.Errorf("resolve output dir: %w", err)
	}
	if err := os.MkdirAll(absOutputDir, 0755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	// 确定主档位（最高清）
	mainProfile := getHighestProfile(profiles)
	fmt.Printf("→ Main profile selected: %s\n", mainProfile)

	//  生成 CQP 探针（仅视频，-qp 23）
	probeTS := filepath.Join(absOutputDir, fmt.Sprintf("temp_probe_%s.ts", jobID))
	if err := generateProbeTS(absInput, mainProfile, probeTS); err != nil {
		return fmt.Errorf("generate CQP probe: %w", err)
	}
	defer func() {
    _ = os.Remove(probeTS) // 忽略"文件不存在"错误
	}()

	// 分析复杂度 → 基础 CRF（使用主档位的参考值）
	complexity, err := analyzeComplexityFromSize(probeTS, mainProfile)
	if err != nil {
		fmt.Printf("⚠️ Probe analysis failed, using default CRF=23: %v\n", err)
		complexity = 1.0
	}
	baseCRF := crfFromComplexity(complexity)
	fmt.Printf("→ Complexity=%.2f → Base CRF=%d\n", complexity, baseCRF)

	//为每个档位全量转码（含音频）
	ctx := context.Background()
	for _, profile := range profiles {
		finalCRF := baseCRF + crfOffset(profile)
		if finalCRF > 28 {
			finalCRF = 28
		}
		fmt.Printf("  → Transcoding %s with CRF=%d (audio: %s)\n", 
			profile, finalCRF, getAudioBitrate(profile))

		if err := transcodeFull(ctx, absInput, profile, jobID, finalCRF); err != nil {
			return fmt.Errorf("transcode profile %s: %w", profile, err)
		}
	}

	//  生成 Master Playlist
	if err := generateMasterPlaylist(profiles, jobID); err != nil {
		return fmt.Errorf("generate master playlist: %w", err)
	}

	fmt.Printf("Transcode completed: %s.m3u8\n", jobID)
	return nil
}