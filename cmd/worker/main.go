package main

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/hibiken/asynq"
	"github.com/joho/godotenv"
	"github.com/oklog/ulid/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
    "github.com/wapuda/uniqueization_pro/internal/logx"

	"github.com/wapuda/uniqueization_pro/internal/jobs"
)

var (
	rctx = context.Background()
)

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func mustInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

type cfg struct {
	DataDir          string
	RatioMin         float64
	RatioMax         float64
	SizeTolerance    float64
	AdaptMaxTries    int
	PerVideoMax      int
	DailyMax         int
	MaxVideosPerMsg  int
	AudioKbpsChoices []int
	JitterProbPct    int
	ScaleJitter      bool
	Concurrency      int
	RedisAddr        string
}

func loadCfg() cfg {
	choices := []int{96, 128, 160, 192}
	if s := os.Getenv("AUDIO_KBPS_SET"); s != "" {
		choices = nil
		for _, p := range strings.Split(s, ",") {
			if x, err := strconv.Atoi(strings.TrimSpace(p)); err == nil && x > 0 {
				choices = append(choices, x)
			}
		}
	}
	return cfg{
		DataDir:          getenv("DATA_DIR", "/data"),
		RatioMin:         mustFloat("RATIO_MIN", 1.05),
		RatioMax:         mustFloat("RATIO_MAX", 1.95),
		SizeTolerance:    mustFloat("SIZE_TOLERANCE", 0.05),
		AdaptMaxTries:    mustInt("ADAPT_MAX_TRIES", 3),
		PerVideoMax:      mustInt("PER_VIDEO_MAX", 50),
		DailyMax:         mustInt("DAILY_MAX", 200),
		MaxVideosPerMsg:  mustInt("MAX_VIDEOS_PER_MSG", 10),
		AudioKbpsChoices: choices,
		JitterProbPct:    mustInt("JITTER_PROB", 80),
		ScaleJitter:      getenv("SCALE_JITTER", "1") == "1",
		Concurrency:      mustInt("CONCURRENCY", 2),
		RedisAddr:        getenv("REDIS_ADDR", "localhost:6379"),
	}
}

func mustFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if x, err := strconv.ParseFloat(v, 64); err == nil {
			return x
		}
	}
	return def
}

func main() {
	_ = godotenv.Load()
	c := loadCfg()

	logx.Setup(logx.FromEnv("worker"))
	logx.Info().Msg("worker starting")

	token := os.Getenv("BOT_TOKEN")
	if token == "" {
		log.Fatal("BOT_TOKEN required")
	}

	// Redis
	rdb := redis.NewClient(&redis.Options{Addr: c.RedisAddr})

	// Asynq worker
	srv := asynq.NewServer(asynq.RedisClientOpt{Addr: c.RedisAddr}, asynq.Config{
		Concurrency: c.Concurrency,
	})

	// Telegram
	bot, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		log.Fatal(err)
	}

	// Mux
	mux := asynq.NewServeMux()
	mux.HandleFunc(jobs.TaskStartSession, func(ctx context.Context, t *asynq.Task) error {
		var p jobs.StartSessionPayload
		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			return err
		}
		return handleStartSession(ctx, bot, rdb, c, p)
	})

	mux.HandleFunc(jobs.TaskEncodeVariant, func(ctx context.Context, t *asynq.Task) error {
		var p jobs.EncodeVariantPayload
		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			return err
		}
		return handleEncodeVariant(ctx, bot, rdb, c, p)
	})

	mux.HandleFunc(jobs.TaskFinalize, func(ctx context.Context, t *asynq.Task) error {
		var p jobs.FinalizePayload
		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			return err
		}
		return handleFinalize(ctx, bot, rdb, c, p)
	})

	logx.Info().Msg("worker v2 starting…")
	if err := srv.Run(mux); err != nil {
		log.Fatal(err)
	}
}

func handleStartSession(ctx context.Context, bot *tgbotapi.BotAPI, rdb *redis.Client, c cfg, p jobs.StartSessionPayload) error {
	// Add context fields (sid/uid) inside handlers
	ctx = context.WithValue(ctx, logx.CtxKeySessionID, p.SessionID)
	ctx = context.WithValue(ctx, logx.CtxKeyUserID, p.UserID)

	logx.FromCtx(ctx).Info().
		Str("format", p.Format).
		Int("amount", p.Amount).
		Int("total", len(p.Items)*p.Amount).
		Msg("session:start accepted")
	// Validate
	f := strings.ToLower(p.Format)
	if f != "mp4" && f != "mov" {
		logx.FromCtx(ctx).Error().Str("format", p.Format).Msg("invalid format selected")
		return sendAndErr(bot, p.ChatID, "Select one format: mp4 or mov.")
	}
	if len(p.Items) == 0 || len(p.Items) > c.MaxVideosPerMsg {
		logx.FromCtx(ctx).Error().Int("items", len(p.Items)).Int("max", c.MaxVideosPerMsg).Msg("invalid number of videos")
		return sendAndErr(bot, p.ChatID, fmt.Sprintf("You can send 1 to %d videos per session.", c.MaxVideosPerMsg))
	}
	if p.Amount < 1 || p.Amount > c.PerVideoMax {
		logx.FromCtx(ctx).Error().Int("amount", p.Amount).Int("max", c.PerVideoMax).Msg("invalid amount per video")
		return sendAndErr(bot, p.ChatID, fmt.Sprintf("Amount must be 1..%d per video.", c.PerVideoMax))
	}
	total := len(p.Items) * p.Amount
	remain, ok, err := checkDailyAllowance(ctx, rdb, c, p.UserID, total)
	if err != nil {
		logx.FromCtx(ctx).Error().Err(err).Msg("failed to check daily allowance")
		return err
	}
	if !ok {
		logx.FromCtx(ctx).Error().Int("requested", total).Int("remaining", remain).Int("daily_max", c.DailyMax).Msg("daily limit exceeded")
		return sendAndErr(bot, p.ChatID, fmt.Sprintf("Daily limit is %d. You have %d left for today. Enter a smaller amount.", c.DailyMax, remain))
	}

	// Session state
	sid := p.SessionID
	if sid == "" {
		sid = newULID()
	}
	base := filepath.Join(c.DataDir, "sessions", sid)
	srcDir := filepath.Join(base, "src")
	outDir := filepath.Join(base, "out")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	// Post initial progress message
	msg := tgbotapi.NewMessage(p.ChatID, fmt.Sprintf("Processing: 0 / %d (0%%)…", total))
	sent, _ := bot.Send(msg)
	if err := saveSessionMeta(ctx, rdb, sid, p, total, sent.MessageID); err != nil {
		return err
	}

	// Download each source once; probe; enqueue variants
	for i, it := range p.Items {
		srcPath, baseName, err := downloadFromTelegram(ctx, os.Getenv("BOT_TOKEN"), it.FileID, srcDir)
		if err != nil {
			logx.FromCtx(ctx).Error().Err(err).Str("file_id", it.FileID).Msg("download from Telegram failed")
			return sendAndErr(bot, p.ChatID, "Download failed: "+err.Error())
		}
		dur, w, h, err := probeVideo(ctx, srcPath)
		if err != nil {
			logx.FromCtx(ctx).Error().Err(err).Str("src_path", srcPath).Msg("video probe failed")
			return sendAndErr(bot, p.ChatID, "Probe failed: "+err.Error())
		}
		_ = w
		_ = h
		info, err := os.Stat(srcPath)
		if err != nil {
			logx.FromCtx(ctx).Error().Err(err).Str("src_path", srcPath).Msg("failed to stat source file")
			return err
		}
		for k := 1; k <= p.Amount; k++ {
			payload := jobs.EncodeVariantPayload{
				SessionID:   sid,
				ChatID:      p.ChatID,
				UserID:      p.UserID,
				Format:      f,
				SrcPath:     srcPath,
				SrcBaseName: baseName,
				DurationSec: dur,
				OrigBytes:   info.Size(),
				Index:       i*p.Amount + k,
				Total:       total,
			}
			b, _ := json.Marshal(payload)
			_, err := asynq.NewClient(asynq.RedisClientOpt{Addr: c.RedisAddr}).
				EnqueueContext(ctx, asynq.NewTask(jobs.TaskEncodeVariant, b), asynq.MaxRetry(5))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func handleEncodeVariant(ctx context.Context, bot *tgbotapi.BotAPI, rdb *redis.Client, c cfg, p jobs.EncodeVariantPayload) error {
	// Add context fields (sid/uid) inside handlers
	ctx = context.WithValue(ctx, logx.CtxKeySessionID, p.SessionID)
	ctx = context.WithValue(ctx, logx.CtxKeyUserID, p.UserID)

	// Choose target size
	r := randRange(c.RatioMin, c.RatioMax)
	targetBytes := int64(float64(p.OrigBytes) * r)
	// Clamp to (S0*1.01 .. S0*2.00)
	minB := int64(float64(p.OrigBytes) * 1.01)
	maxB := p.OrigBytes * 2
	if targetBytes < minB {
		targetBytes = minB
	}
	if targetBytes > maxB {
		targetBytes = maxB
	}

	// Encode with adaptive loop
	outDir := filepath.Join(c.DataDir, "sessions", p.SessionID, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		logx.FromCtx(ctx).Error().Err(err).Str("out_dir", outDir).Msg("failed to create output directory")
		return err
	}
	outPath, err := encodeAdaptive(ctx, p, outDir, targetBytes, c)
	if err != nil {
		logx.FromCtx(ctx).Error().Err(err).Str("src_path", p.SrcPath).Str("out_dir", outDir).Msg("encode adaptive failed")
		return err
	}

	// Record output path & progress
	if err := rdb.RPush(ctx, keySessionOuts(p.SessionID), outPath).Err(); err != nil {
		return err
	}
	done, total, mid, err := incProgress(ctx, rdb, p.SessionID)
	if err != nil {
		return err
	}
	percent := int(math.Round(float64(done) * 100 / float64(total)))
	edit := tgbotapi.NewEditMessageText(p.ChatID, mid, fmt.Sprintf("Processing: %d / %d (%d%%)…", done, total, percent))
	_, _ = bot.Send(edit)

	if done == total {
		// Fire finalize
		fp := jobs.FinalizePayload{SessionID: p.SessionID, ChatID: p.ChatID, UserID: p.UserID, Total: total}
		b, _ := json.Marshal(fp)
		_, err := asynq.NewClient(asynq.RedisClientOpt{Addr: c.RedisAddr}).
			EnqueueContext(ctx, asynq.NewTask(jobs.TaskFinalize, b), asynq.MaxRetry(3))
		return err
	}
	return nil
}

func handleFinalize(ctx context.Context, bot *tgbotapi.BotAPI, rdb *redis.Client, c cfg, p jobs.FinalizePayload) error {
	// Add context fields (sid/uid) inside handlers
	ctx = context.WithValue(ctx, logx.CtxKeySessionID, p.SessionID)
	ctx = context.WithValue(ctx, logx.CtxKeyUserID, p.UserID)

	// Build ZIP
	base := filepath.Join(c.DataDir, "sessions", p.SessionID)
	outDir := filepath.Join(base, "out")
	paths, err := rdb.LRange(ctx, keySessionOuts(p.SessionID), 0, -1).Result()
	if err != nil {
		logx.FromCtx(ctx).Error().Err(err).Msg("failed to get session outputs from Redis")
		return err
	}
	if len(paths) == 0 {
		logx.FromCtx(ctx).Error().Msg("no outputs to zip")
		return errors.New("no outputs to zip")
	}
	zipPath := filepath.Join(base, fmt.Sprintf("UNIQ-%s.zip", p.SessionID))
	if err := makeZip(zipPath, outDir, paths); err != nil {
		logx.FromCtx(ctx).Error().Err(err).Str("zip_path", zipPath).Int("files", len(paths)).Msg("failed to create ZIP")
		return err
	}

	// Send one ZIP
	doc := tgbotapi.NewDocument(p.ChatID, tgbotapi.FilePath(zipPath))
	doc.Caption = "Your unified videos (ZIP)"
	if _, err := bot.Send(doc); err != nil {
		logx.FromCtx(ctx).Error().Err(err).Str("zip_path", zipPath).Msg("failed to send ZIP to Telegram")
		return err
	}

	// Charge daily quota & finish message
	if err := chargeDaily(ctx, rdb, c, p.UserID, p.Total); err != nil {
		logx.FromCtx(ctx).Error().Err(err).Int("total", p.Total).Msg("quota charge error")
	}
	_, _, mid, _ := getProgress(ctx, rdb, p.SessionID)
	final := tgbotapi.NewEditMessageText(p.ChatID, mid, fmt.Sprintf("Done: %d / %d (100%%).", p.Total, p.Total))
	_, _ = bot.Send(final)

	return nil
}

// --- Encoding core ---

type probeInfo struct {
	Duration float64
	Width    int
	Height   int
}

func probeVideo(ctx context.Context, path string) (dur float64, w, h int, err error) {
	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error",
		"-select_streams", "v:0",
		"-show_entries", "format=duration:stream=width,height",
		"-of", "json", path)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err = cmd.Run(); err != nil {
		return
	}
	var obj struct {
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
		Streams []struct {
			Width  int `json:"width"`
			Height int `json:"height"`
		} `json:"streams"`
	}
	if err = json.Unmarshal(out.Bytes(), &obj); err != nil {
		return
	}
	if len(obj.Streams) == 0 {
		err = errors.New("no video stream")
		return
	}
	w, h = obj.Streams[0].Width, obj.Streams[0].Height
	dur, _ = strconv.ParseFloat(obj.Format.Duration, 64)
	if dur <= 0 {
		err = errors.New("invalid duration")
	}
	return
}

func encodeAdaptive(ctx context.Context, p jobs.EncodeVariantPayload, outDir string, targetBytes int64, c cfg) (string, error) {
	// unique name
	h := sha256.New()
	_ = hashFile(p.SrcPath, h)
	short := hex.EncodeToString(h.Sum(nil))[:8]
	w, hgt := 0, 0
	if d, w0, h0, err := probeVideo(ctx, p.SrcPath); err == nil && d > 0 {
		_ = d
		w, hgt = w0, h0
	}
	id := newULID()
	ext := p.Format
	baseName := fmt.Sprintf("vid-%s-%s-%dx%d.%s", id, short, w, hgt, ext)
	outPath := filepath.Join(outDir, baseName)

	// Log encode attempt
	logx.FromCtx(ctx).Info().
		Str("src", p.SrcPath).
		Str("out", outPath).
		Str("format", p.Format).
		Int64("target_bytes", targetBytes).
		Int("width", w).
		Int("height", hgt).
		Msg("encode attempt")

	// pick audio kbps & preset/gop
	abr := c.AudioKbpsChoices[rand.Intn(len(c.AudioKbpsChoices))]
	preset := pick([]string{"veryfast", "faster", "fast", "medium", "slow"})
	gop := 48 + rand.Intn(73) // 48..120

	// derive initial video bitrate from targetBytes & duration
	totalBps := float64(targetBytes*8) / p.DurationSec
	vbps := int64(totalBps) - int64(abr*1000)
	if vbps < 64_000 {
		vbps = 64_000
	}

	// jitter?
	scaleW, scaleH := 0, 0
	if c.ScaleJitter && rand.Intn(100) < c.JitterProbPct && w > 0 && hgt > 0 {
		f := 97 + rand.Intn(7) // 97..103 %
		scaleW = even((w * f) / 100)
		scaleH = even((hgt * f) / 100)
	}

	// attempts
	tryV := vbps
	for i := 0; i < c.AdaptMaxTries; i++ {
		if err := runFFmpegOnce(ctx, p.SrcPath, outPath, p.Format, tryV, abr, preset, gop, scaleW, scaleH); err != nil {
			logx.FromCtx(ctx).Error().Err(err).Int("attempt", i+1).Int64("vbit", tryV).Msg("ffmpeg attempt failed")
			return "", err
		}
		sz, _ := fileSize(outPath)
		if sz > p.OrigBytes && sz <= 2*p.OrigBytes {
			// good enough or close to target?
			diff := math.Abs(float64(sz-targetBytes)) / float64(targetBytes)
			if diff <= c.SizeTolerance {
				logx.FromCtx(ctx).Info().Int("attempt", i+1).Int64("size", sz).Int64("target", targetBytes).Float64("diff", diff).Msg("encode successful")
				return outPath, nil
			}
			// else adjust bitrate toward target
		}
		// adjust bitrate
		if sz <= p.OrigBytes {
			f := math.Max(1.10, float64(targetBytes)/float64(sz))
			tryV = int64(float64(tryV) * f)
		} else if sz > 2*p.OrigBytes {
			f := math.Min(0.90, float64(targetBytes)/float64(sz))
			tryV = int64(float64(tryV) * f)
		} else {
			// in band but not close enough → small nudges
			if float64(sz) < float64(targetBytes) {
				tryV = int64(float64(tryV) * 1.08)
			} else {
				tryV = int64(float64(tryV) * 0.92)
			}
		}
	}

	// fallback: 2-pass to hit target tightly
	passlog := outPath + ".log"
	logx.FromCtx(ctx).Info().Msg("falling back to two-pass encoding")
	if err := runTwoPass(ctx, p.SrcPath, outPath, p.Format, targetBytes, abr, preset, gop, scaleW, scaleH, passlog); err != nil {
		logx.FromCtx(ctx).Error().Err(err).Msg("two-pass encoding failed")
		return "", err
	}
	sz, _ := fileSize(outPath)
	if !(sz > p.OrigBytes && sz <= 2*p.OrigBytes) {
		logx.FromCtx(ctx).Error().Int64("size", sz).Int64("min", p.OrigBytes+1).Int64("max", 2*p.OrigBytes).Msg("final size out of band")
		return "", fmt.Errorf("final size out of band: got %d vs [%d..%d]", sz, p.OrigBytes+1, 2*p.OrigBytes)
	}
	logx.FromCtx(ctx).Info().Int64("size", sz).Int64("target", targetBytes).Msg("two-pass encoding successful")
	return outPath, nil
}

func runFFmpegOnce(ctx context.Context, in, out, format string, vbit int64, akbps int, preset string, gop int, scaleW, scaleH int) error {
	_ = os.Remove(out)
	args := []string{"-nostdin", "-y", "-i", in}
	// filters
	vf := []string{"noise=alls=1:allf=t"}
	if scaleW > 0 && scaleH > 0 {
		vf = append([]string{fmt.Sprintf("scale=%d:%d", scaleW, scaleH)}, vf...)
	}
	args = append(args, "-vf", strings.Join(vf, ","))

	args = append(args,
		"-c:v", "libx264",
		"-b:v", fmt.Sprintf("%d", vbit),
		"-maxrate", fmt.Sprintf("%d", vbit),
		"-bufsize", fmt.Sprintf("%d", vbit*2),
		"-preset", preset,
		"-g", strconv.Itoa(gop), "-keyint_min", strconv.Itoa(gop),
		"-pix_fmt", "yuv420p",
		"-map", "0:v:0", "-map", "0:a?",
		"-c:a", "aac", "-b:a", fmt.Sprintf("%dk", akbps), "-ac", "2", "-ar", "48000",
		"-map_metadata", "-1", "-map_chapters", "-1",
	)
	if format == "mp4" {
		args = append(args, "-movflags", "+faststart")
	}
	args = append(args, out)

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)

	// capture stderr at debug level
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		logx.FromCtx(ctx).Error().Err(err).Msg("ffmpeg start failed")
		return err
	}
	go logx.NewLineWriter(map[string]string{
		"proc": "ffmpeg",
		"step": "onepass",
	}, zerolog.DebugLevel).Pipe(stderr)

	if err := cmd.Wait(); err != nil {
		logx.FromCtx(ctx).Error().Err(err).Msg("ffmpeg failed")
		return err
	}
	return nil
}

func runTwoPass(ctx context.Context, in, out, format string, targetBytes int64, akbps int, preset string, gop int, scaleW, scaleH int, passlog string) error {
	// derive bitrate from targetBytes and dummy duration via ffprobe
	dur, _, _, err := probeVideo(ctx, in)
	if err != nil || dur <= 0 {
		return errors.New("two-pass probe failed")
	}
	totalBps := float64(targetBytes*8) / dur
	vbps := int64(totalBps) - int64(akbps*1000)
	if vbps < 64_000 {
		vbps = 64_000
	}
	// pass 1
	_ = os.Remove(passlog + "-0.log")
	_ = os.Remove(out)
	vf := []string{"noise=alls=1:allf=t"}
	if scaleW > 0 && scaleH > 0 {
		vf = append([]string{fmt.Sprintf("scale=%d:%d", scaleW, scaleH)}, vf...)
	}
	// pass 1 (no audio)
	args1 := []string{
		"-nostdin", "-y", "-i", in,
		"-vf", strings.Join(vf, ","),
		"-c:v", "libx264",
		"-b:v", fmt.Sprintf("%d", vbps),
		"-maxrate", fmt.Sprintf("%d", vbps),
		"-bufsize", fmt.Sprintf("%d", vbps*2),
		"-preset", preset,
		"-g", strconv.Itoa(gop), "-keyint_min", strconv.Itoa(gop),
		"-pix_fmt", "yuv420p",
		"-map", "0:v:0",
		"-an",
		"-map_metadata", "-1", "-map_chapters", "-1",
		"-pass", "1", "-passlogfile", passlog,
		"-f", "mp4", os.DevNull,
	}
	cmd1 := exec.CommandContext(ctx, "ffmpeg", args1...)
	
	// capture stderr at debug level for pass 1
	stderr1, _ := cmd1.StderrPipe()
	if err := cmd1.Start(); err != nil {
		logx.FromCtx(ctx).Error().Err(err).Msg("ffmpeg pass 1 start failed")
		return err
	}
	go logx.NewLineWriter(map[string]string{
		"proc": "ffmpeg",
		"step": "twopass1",
	}, zerolog.DebugLevel).Pipe(stderr1)

	if err := cmd1.Wait(); err != nil {
		logx.FromCtx(ctx).Error().Err(err).Msg("ffmpeg pass 1 failed")
		return err
	}
	
	// pass 2 (with audio)
	args2 := []string{
		"-nostdin", "-y", "-i", in,
		"-vf", strings.Join(vf, ","),
		"-c:v", "libx264",
		"-b:v", fmt.Sprintf("%d", vbps),
		"-maxrate", fmt.Sprintf("%d", vbps),
		"-bufsize", fmt.Sprintf("%d", vbps*2),
		"-preset", preset,
		"-g", strconv.Itoa(gop), "-keyint_min", strconv.Itoa(gop),
		"-pix_fmt", "yuv420p",
		"-map", "0:v:0", "-map", "0:a?",
		"-c:a", "aac", "-b:a", fmt.Sprintf("%dk", akbps), "-ac", "2", "-ar", "48000",
		"-map_metadata", "-1", "-map_chapters", "-1",
		"-pass", "2", "-passlogfile", passlog,
	}
	if format == "mp4" {
		args2 = append(args2, "-movflags", "+faststart")
	}
	args2 = append(args2, out)
	cmd2 := exec.CommandContext(ctx, "ffmpeg", args2...)
	
	// capture stderr at debug level for pass 2
	stderr2, _ := cmd2.StderrPipe()
	if err := cmd2.Start(); err != nil {
		logx.FromCtx(ctx).Error().Err(err).Msg("ffmpeg pass 2 start failed")
		return err
	}
	go logx.NewLineWriter(map[string]string{
		"proc": "ffmpeg",
		"step": "twopass2",
	}, zerolog.DebugLevel).Pipe(stderr2)

	if err := cmd2.Wait(); err != nil {
		logx.FromCtx(ctx).Error().Err(err).Msg("ffmpeg pass 2 failed")
		return err
	}
	return nil
}

// --- Telegram I/O, ZIP, Redis helpers ---

func downloadFromTelegram(ctx context.Context, token, fileID, dir string) (string, string, error) {
	type fileRes struct {
		OK     bool `json:"ok"`
		Result struct {
			FilePath string `json:"file_path"`
		} `json:"result"`
	}
	u := fmt.Sprintf("https://api.telegram.org/bot%s/getFile?file_id=%s", token, fileID)
	req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	var fr fileRes
	if err := json.NewDecoder(resp.Body).Decode(&fr); err != nil {
		return "", "", err
	}
	if !fr.OK || fr.Result.FilePath == "" {
		return "", "", errors.New("getFile failed")
	}
	fileURL := fmt.Sprintf("https://api.telegram.org/file/bot%s/%s", token, fr.Result.FilePath)
	req2, _ := http.NewRequestWithContext(ctx, "GET", fileURL, nil)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		return "", "", err
	}
	defer resp2.Body.Close()

	// keep original name for caption
	baseName := filepath.Base(fr.Result.FilePath)
	ext := filepath.Ext(baseName)
	if ext == "" {
		ext = ".mp4"
	}
	sid := newULID()
	dst := filepath.Join(dir, fmt.Sprintf("src-%s%s", sid, ext))

	f, err := os.Create(dst)
	if err != nil {
		return "", "", err
	}
	defer f.Close()
	if _, err := io.Copy(f, resp2.Body); err != nil {
		return "", "", err
	}
	return dst, baseName, nil
}

func makeZip(zipPath, baseDir string, paths []string) error {
	_ = os.Remove(zipPath)
	zf, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer zf.Close()
	zw := zip.NewWriter(zf)
	defer zw.Close()

	for _, p := range paths {
		rel := filepath.Base(p)
		if strings.HasPrefix(p, baseDir) {
			rel = filepath.Join("out", filepath.Base(p))
		}
		w, err := zw.Create(rel)
		if err != nil {
			return err
		}
		in, err := os.Open(p)
		if err != nil {
			return err
		}
		if _, err := io.Copy(w, in); err != nil {
			in.Close()
			return err
		}
		in.Close()
	}
	return nil
}

func keyQuota(user int64, yyyymmdd string) string {
	return fmt.Sprintf("quota:%d:%s", user, yyyymmdd)
}
func keySessionMeta(sid string) string { return "sess:" + sid + ":meta" }
func keySessionProg(sid string) string { return "sess:" + sid + ":prog" }
func keySessionOuts(sid string) string { return "sess:" + sid + ":outs" }

func today() string { return time.Now().Format("20060102") }

func ttlUntilMidnight() time.Duration {
	now := time.Now()
	tom := now.Add(24 * time.Hour)
	mid := time.Date(tom.Year(), tom.Month(), tom.Day(), 0, 0, 0, 0, now.Location())
	return time.Until(mid)
}

func checkDailyAllowance(ctx context.Context, rdb *redis.Client, c cfg, user int64, want int) (remain int, ok bool, err error) {
	key := keyQuota(user, today())
	used, _ := rdb.Get(ctx, key).Int()
	if used >= c.DailyMax {
		return 0, false, nil
	}
	remain = c.DailyMax - used
	ok = want <= remain
	return
}

func chargeDaily(ctx context.Context, rdb *redis.Client, c cfg, user int64, add int) error {
	key := keyQuota(user, today())
	pipe := rdb.TxPipeline()
	pipe.IncrBy(ctx, key, int64(add))
	pipe.Expire(ctx, key, ttlUntilMidnight())
	_, err := pipe.Exec(ctx)
	return err
}

func saveSessionMeta(ctx context.Context, rdb *redis.Client, sid string, p jobs.StartSessionPayload, total int, msgID int) error {
	pipe := rdb.TxPipeline()
	pipe.HSet(ctx, keySessionMeta(sid), map[string]interface{}{
		"chat_id": p.ChatID, "user_id": p.UserID, "format": strings.ToLower(p.Format),
		"total": total, "msg_id": msgID,
	})
	pipe.Set(ctx, keySessionProg(sid), 0, 24*time.Hour)
	_, err := pipe.Exec(ctx)
	return err
}

func incProgress(ctx context.Context, rdb *redis.Client, sid string) (done, total, msgID int, err error) {
	pipe := rdb.TxPipeline()
	inc := pipe.Incr(ctx, keySessionProg(sid))
	meta := pipe.HGetAll(ctx, keySessionMeta(sid))
	if _, e := pipe.Exec(ctx); e != nil {
		err = e
		return
	}
	done = int(inc.Val())
	tStr := meta.Val()["total"]
	msgStr := meta.Val()["msg_id"]
	total, _ = strconv.Atoi(tStr)
	msgID, _ = strconv.Atoi(msgStr)
	return
}

func getProgress(ctx context.Context, rdb *redis.Client, sid string) (done, total, msgID int, err error) {
	meta, err := rdb.HGetAll(ctx, keySessionMeta(sid)).Result()
	if err != nil {
		return
	}
	ps, _ := rdb.Get(ctx, keySessionProg(sid)).Int()
	tStr := meta["total"]
	msgStr := meta["msg_id"]
	done = ps
	total, _ = strconv.Atoi(tStr)
	msgID, _ = strconv.Atoi(msgStr)
	return
}

func sendAndErr(bot *tgbotapi.BotAPI, chat int64, text string) error {
	_, _ = bot.Send(tgbotapi.NewMessage(chat, text))
	return errors.New(text)
}

func fileSize(p string) (int64, error) {
	info, err := os.Stat(p)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func hashFile(path string, h io.Writer) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(h.(hashWriter), f)
	return err
}

type hashWriter interface {
	Write(p []byte) (n int, err error)
	Sum(b []byte) []byte
}

func newULID() string {
	t := time.Now()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	return ulid.MustNew(ulid.Timestamp(t), entropy).String()
}

func pick[T any](arr []T) T { return arr[rand.Intn(len(arr))] }

func randRange(a, b float64) float64 {
	return a + rand.Float64()*(b-a)
}

func even(x int) int {
	if x%2 == 0 {
		return x
	}
	return x - 1
}
