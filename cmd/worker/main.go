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
	"net/url"
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

	"github.com/wapuda/uniqueization_pro/internal/jobs"
)

/* ---------------------- config & utils ---------------------- */

type cfg struct {
	DataDir               string
	RatioMin              float64
	RatioMax              float64
	SizeTolerance         float64
	AdaptMaxTries         int
	PerVideoMax           int
	DailyMax              int
	MaxVideosPerMsg       int
	AudioKbpsChoices      []int
	JitterProbPct         int
	ScaleJitter           bool
	Concurrency           int
	RedisAddr             string
	FilebinEnable         bool
	FilebinAlways         bool
	FilebinBase           string
	FilebinBinPrefix      string
	TelegramUploadMaxByte int64
	BotToken              string
}

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
func mustFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if x, err := strconv.ParseFloat(v, 64); err == nil {
			return x
		}
	}
	return def
}
func mustBool(k string, def bool) bool {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
	}
	return def
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
	mb := mustInt("TG_UPLOAD_LIMIT_MB", 49)
	return cfg{
		DataDir:               getenv("DATA_DIR", "/data"),
		RatioMin:              mustFloat("RATIO_MIN", 1.05),
		RatioMax:              mustFloat("RATIO_MAX", 1.95),
		SizeTolerance:         mustFloat("SIZE_TOLERANCE", 0.05),
		AdaptMaxTries:         mustInt("ADAPT_MAX_TRIES", 3),
		PerVideoMax:           mustInt("PER_VIDEO_MAX", 50),
		DailyMax:              mustInt("DAILY_MAX", 200),
		MaxVideosPerMsg:       mustInt("MAX_VIDEOS_PER_MSG", 10),
		AudioKbpsChoices:      choices,
		JitterProbPct:         mustInt("JITTER_PROB", 80),
		ScaleJitter:           getenv("SCALE_JITTER", "1") == "1",
		Concurrency:           mustInt("CONCURRENCY", 2),
		RedisAddr:             getenv("REDIS_ADDR", "localhost:6379"),
		FilebinEnable:         mustBool("FILEBIN_ENABLE", false),
		FilebinAlways:         mustBool("FILEBIN_ALWAYS", false),
		FilebinBase:           strings.TrimRight(getenv("FILEBIN_BASE", "https://filebin.net"), "/"),
		FilebinBinPrefix:      getenv("FILEBIN_BIN_PREFIX", ""),
		TelegramUploadMaxByte: int64(mb) * 1024 * 1024,
		BotToken:              os.Getenv("BOT_TOKEN"),
	}
}

func newULID() string {
	t := time.Now()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	return ulid.MustNew(ulid.Timestamp(t), entropy).String()
}
func even(x int) int {
	if x%2 == 0 {
		return x
	}
	return x - 1
}
func pick[T any](arr []T) T          { return arr[rand.Intn(len(arr))] }
func randRange(a, b float64) float64 { return a + rand.Float64()*(b-a) }

/* ---------------------- main ---------------------- */

func main() {
	_ = godotenv.Load()
	c := loadCfg()
	if c.BotToken == "" {
		log.Fatal("BOT_TOKEN required")
	}
	if err := os.MkdirAll(filepath.Join(c.DataDir, "sessions"), 0o755); err != nil {
		log.Fatal(err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: c.RedisAddr})
	srv := asynq.NewServer(asynq.RedisClientOpt{Addr: c.RedisAddr}, asynq.Config{Concurrency: c.Concurrency})
	bot, err := tgbotapi.NewBotAPI(c.BotToken)
	if err != nil {
		log.Fatal(err)
	}

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

	log.Println("worker v2 (filebin-enabled) starting…")
	if err := srv.Run(mux); err != nil {
		log.Fatal(err)
	}
}

/* ---------------------- session start ---------------------- */

func handleStartSession(ctx context.Context, bot *tgbotapi.BotAPI, rdb *redis.Client, c cfg, p jobs.StartSessionPayload) error {
	f := strings.ToLower(p.Format)
	if f != "mp4" && f != "mov" {
		return sendAndErr(bot, p.ChatID, "Select one format: mp4 or mov.")
	}
	if len(p.Items) == 0 || len(p.Items) > c.MaxVideosPerMsg {
		return sendAndErr(bot, p.ChatID, fmt.Sprintf("You can send 1 to %d videos per session.", c.MaxVideosPerMsg))
	}
	if p.Amount < 1 || p.Amount > c.PerVideoMax {
		return sendAndErr(bot, p.ChatID, fmt.Sprintf("Amount must be 1..%d per video.", c.PerVideoMax))
	}
	total := len(p.Items) * p.Amount
	remain, ok, err := checkDailyAllowance(ctx, rdb, c, p.UserID, total)
	if err != nil {
		return err
	}
	if !ok {
		return sendAndErr(bot, p.ChatID, fmt.Sprintf("Daily limit is %d. You have %d left.", c.DailyMax, remain))
	}

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

	msg := tgbotapi.NewMessage(p.ChatID, fmt.Sprintf("Processing: 0 / %d (0%%)…", total))
	sent, _ := bot.Send(msg)
	if err := saveSessionMeta(ctx, rdb, sid, p, total, sent.MessageID); err != nil {
		return err
	}

	client := asynq.NewClient(asynq.RedisClientOpt{Addr: c.RedisAddr})
	defer client.Close()
	for i, it := range p.Items {
		srcPath, baseName, err := downloadFromTelegram(ctx, c.BotToken, it.FileID, srcDir)
		if err != nil {
			return sendAndErr(bot, p.ChatID, "Download failed: "+err.Error())
		}
		dur, w, h, err := probeVideo(ctx, srcPath)
		if err != nil {
			return sendAndErr(bot, p.ChatID, "Probe failed: "+err.Error())
		}
		_ = w
		_ = h
		info, err := os.Stat(srcPath)
		if err != nil {
			return err
		}
		for k := 1; k <= p.Amount; k++ {
			payload := jobs.EncodeVariantPayload{
				SessionID: sid, ChatID: p.ChatID, UserID: p.UserID, Format: f,
				SrcPath: srcPath, SrcBaseName: baseName, DurationSec: dur, OrigBytes: info.Size(),
				Index: i*p.Amount + k, Total: total,
			}
			b, _ := json.Marshal(payload)
			if _, err := client.EnqueueContext(ctx, asynq.NewTask(jobs.TaskEncodeVariant, b), asynq.MaxRetry(5)); err != nil {
				return err
			}
		}
	}
	return nil
}

/* ---------------------- variant encoding ---------------------- */

func handleEncodeVariant(ctx context.Context, bot *tgbotapi.BotAPI, rdb *redis.Client, c cfg, p jobs.EncodeVariantPayload) error {
	r := randRange(c.RatioMin, c.RatioMax)
	target := int64(float64(p.OrigBytes) * r)
	minB := int64(float64(p.OrigBytes) * 1.01)
	maxB := p.OrigBytes * 2
	if target < minB {
		target = minB
	}
	if target > maxB {
		target = maxB
	}

	outDir := filepath.Join(c.DataDir, "sessions", p.SessionID, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	outPath, err := encodeAdaptive(ctx, p, outDir, target, c)
	if err != nil {
		return err
	}

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
		fp := jobs.FinalizePayload{SessionID: p.SessionID, ChatID: p.ChatID, UserID: p.UserID, Total: total}
		b, _ := json.Marshal(fp)
		_, err := asynq.NewClient(asynq.RedisClientOpt{Addr: c.RedisAddr}).EnqueueContext(ctx, asynq.NewTask(jobs.TaskFinalize, b), asynq.MaxRetry(3))
		return err
	}
	return nil
}

/* ---------------------- finalize (ZIP + Filebin/Telegram) ---------------------- */

func handleFinalize(ctx context.Context, bot *tgbotapi.BotAPI, rdb *redis.Client, c cfg, p jobs.FinalizePayload) error {
	base := filepath.Join(c.DataDir, "sessions", p.SessionID)
	outDir := filepath.Join(base, "out")
	paths, err := rdb.LRange(ctx, keySessionOuts(p.SessionID), 0, -1).Result()
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return errors.New("no outputs to zip")
	}

	zipName := fmt.Sprintf("UNIQ-%s.zip", p.SessionID)
	zipPath := filepath.Join(base, zipName)
	if err := makeZip(zipPath, outDir, paths); err != nil {
		return err
	}
	size, _ := fileSize(zipPath)

	useFilebin := c.FilebinEnable && (c.FilebinAlways || size > c.TelegramUploadMaxByte)
	if useFilebin {
		bin := p.SessionID
		if pref := strings.TrimSpace(c.FilebinBinPrefix); pref != "" {
			bin = pref + "-" + p.SessionID
		}
		fileURL, binURL, upErr := uploadToFilebin(ctx, c.FilebinBase, bin, zipPath, zipName)
		if upErr != nil {
			log.Printf("filebin upload failed: %v (size=%d)", upErr, size)
			if size <= c.TelegramUploadMaxByte {
				return sendZipViaTelegram(bot, p.ChatID, zipPath, p.Total, p.SessionID, rdb)
			}
			_, _, mid, _ := getProgress(ctx, rdb, p.SessionID)
			_, _ = bot.Send(tgbotapi.NewEditMessageText(p.ChatID, mid,
				fmt.Sprintf("Finished %d videos.\nCould not upload ZIP to Filebin: %v", p.Total, upErr)))
			return nil
		}
		_, _, mid, _ := getProgress(ctx, rdb, p.SessionID)
		text := fmt.Sprintf("Done: %d / %d (100%%).\nDownload your ZIP:\n%s\n(Bin: %s)\nNote: link expires in ~6 days.", p.Total, p.Total, fileURL, binURL)
		_, _ = bot.Send(tgbotapi.NewEditMessageText(p.ChatID, mid, text))
		_ = chargeDaily(ctx, rdb, c, p.UserID, p.Total)
		return nil
	}
	return sendZipViaTelegram(bot, p.ChatID, zipPath, p.Total, p.SessionID, rdb)
}

func sendZipViaTelegram(bot *tgbotapi.BotAPI, chatID int64, zipPath string, total int, sid string, rdb *redis.Client) error {
	doc := tgbotapi.NewDocument(chatID, tgbotapi.FilePath(zipPath))
	doc.Caption = "Your unified videos (ZIP)"
	if _, err := bot.Send(doc); err != nil {
		return err
	}
	_, _, mid, _ := getProgress(context.Background(), rdb, sid)
	_, _ = bot.Send(tgbotapi.NewEditMessageText(chatID, mid, fmt.Sprintf("Done: %d / %d (100%%).", total, total)))
	return nil
}

/* ---------------------- Filebin client ---------------------- */

func uploadToFilebin(ctx context.Context, base, bin, path, filename string) (fileURL, binURL string, err error) {
	u := strings.TrimRight(base, "/") + "/"
	f, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer f.Close()

	req, _ := http.NewRequestWithContext(ctx, "POST", u, f)
	req.Header.Set("Content-Type", "application/zip")
	req.Header.Set("Accept", "application/json")
	if filename != "" {
		req.Header.Set("filename", filename)
	}
	if bin != "" {
		req.Header.Set("bin", bin)
	}

	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("filebin status=%d body=%s", resp.StatusCode, substr(string(body), 256))
	}
	if bin == "" {
		var obj map[string]any
		if json.Unmarshal(body, &obj) == nil {
			if id, ok := obj["id"].(string); ok && id != "" {
				bin = id
			}
		}
		if bin == "" {
			return "", "", fmt.Errorf("filebin upload ok but could not determine bin id")
		}
	}
	fileURL = fmt.Sprintf("%s/%s/%s", strings.TrimRight(base, "/"), bin, url.PathEscape(filename))
	binURL = fmt.Sprintf("%s/%s", strings.TrimRight(base, "/"), bin)
	return fileURL, binURL, nil
}

/* ---------------------- ffprobe/ffmpeg ---------------------- */

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
		Streams []struct{ Width, Height int } `json:"streams"`
	}
	if err = json.Unmarshal(out.Bytes(), &obj); err != nil {
		return
	}
	if len(obj.Streams) == 0 {
		return 0, 0, 0, errors.New("no video stream")
	}
	w, h = obj.Streams[0].Width, obj.Streams[0].Height
	dur, _ = strconv.ParseFloat(obj.Format.Duration, 64)
	if dur <= 0 {
		err = errors.New("invalid duration")
	}
	return
}

func encodeAdaptive(ctx context.Context, p jobs.EncodeVariantPayload, outDir string, targetBytes int64, c cfg) (string, error) {
	short, _ := shortHash(p.SrcPath)
	_, w0, h0, _ := probeVideo(ctx, p.SrcPath)
	id := newULID()
	ext := p.Format
	baseName := fmt.Sprintf("vid-%s-%s-%dx%d.%s", id, short, w0, h0, ext)
	outPath := filepath.Join(outDir, baseName)

	abr := c.AudioKbpsChoices[rand.Intn(len(c.AudioKbpsChoices))]
	preset := pick([]string{"veryfast", "faster", "fast", "medium", "slow"})
	gop := 48 + rand.Intn(73)
	totalBps := float64(targetBytes*8) / p.DurationSec
	vbps := int64(totalBps) - int64(abr*1000)
	if vbps < 64_000 {
		vbps = 64_000
	}

	scaleW, scaleH := 0, 0
	if c.ScaleJitter && rand.Intn(100) < c.JitterProbPct && w0 > 0 && h0 > 0 {
		f := 97 + rand.Intn(7)
		scaleW = even((w0 * f) / 100)
		scaleH = even((h0 * f) / 100)
	}

	tryV := vbps
	for i := 0; i < c.AdaptMaxTries; i++ {
		if err := runFFmpegOnce(ctx, p.SrcPath, outPath, p.Format, tryV, abr, preset, gop, scaleW, scaleH); err != nil {
			return "", err
		}
		sz, _ := fileSize(outPath)
		if sz > p.OrigBytes && sz <= 2*p.OrigBytes {
			diff := math.Abs(float64(sz-targetBytes)) / float64(targetBytes)
			if diff <= c.SizeTolerance {
				return outPath, nil
			}
		}
		if sz <= p.OrigBytes {
			f := math.Max(1.10, float64(targetBytes)/float64(sz))
			tryV = int64(float64(tryV) * f)
		} else if sz > 2*p.OrigBytes {
			f := math.Min(0.90, float64(targetBytes)/float64(sz))
			tryV = int64(float64(tryV) * f)
		} else {
			if float64(sz) < float64(targetBytes) {
				tryV = int64(float64(tryV) * 1.08)
			} else {
				tryV = int64(float64(tryV) * 0.92)
			}
		}
	}
	passlog := outPath + ".log"
	if err := runTwoPass(ctx, p.SrcPath, outPath, p.Format, targetBytes, abr, preset, gop, scaleW, scaleH, passlog); err != nil {
		return "", err
	}
	sz, _ := fileSize(outPath)
	if !(sz > p.OrigBytes && sz <= 2*p.OrigBytes) {
		return "", fmt.Errorf("final size out of band: got %d vs [%d..%d]", sz, p.OrigBytes+1, 2*p.OrigBytes)
	}
	return outPath, nil
}

func runFFmpegOnce(ctx context.Context, in, out, format string, vbit int64, akbps int, preset string, gop int, scaleW, scaleH int) error {
	_ = os.Remove(out)
	args := []string{"-nostdin", "-y", "-i", in}
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
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}

func runTwoPass(ctx context.Context, in, out, format string, targetBytes int64, akbps int, preset string, gop int, scaleW, scaleH int, passlog string) error {
	dur, _, _, err := probeVideo(ctx, in)
	if err != nil || dur <= 0 {
		return errors.New("two-pass probe failed")
	}
	totalBps := float64(targetBytes*8) / dur
	vbps := int64(totalBps) - int64(akbps*1000)
	if vbps < 64_000 {
		vbps = 64_000
	}

	_ = os.Remove(passlog + "-0.log")
	_ = os.Remove(out)
	vf := []string{"noise=alls=1:allf=t"}
	if scaleW > 0 && scaleH > 0 {
		vf = append([]string{fmt.Sprintf("scale=%d:%d", scaleW, scaleH)}, vf...)
	}

	args1 := []string{"-nostdin", "-y", "-i", in,
		"-vf", strings.Join(vf, ","),
		"-c:v", "libx264", "-b:v", fmt.Sprintf("%d", vbps),
		"-maxrate", fmt.Sprintf("%d", vbps), "-bufsize", fmt.Sprintf("%d", vbps*2),
		"-preset", preset, "-g", strconv.Itoa(gop), "-keyint_min", strconv.Itoa(gop),
		"-pix_fmt", "yuv420p", "-map", "0:v:0", "-an",
		"-map_metadata", "-1", "-map_chapters", "-1",
		"-pass", "1", "-passlogfile", passlog, "-f", "mp4", os.DevNull,
	}
	if err := exec.CommandContext(ctx, "ffmpeg", args1...).Run(); err != nil {
		return err
	}

	args2 := []string{"-nostdin", "-y", "-i", in,
		"-vf", strings.Join(vf, ","),
		"-c:v", "libx264", "-b:v", fmt.Sprintf("%d", vbps),
		"-maxrate", fmt.Sprintf("%d", vbps), "-bufsize", fmt.Sprintf("%d", vbps*2),
		"-preset", preset, "-g", strconv.Itoa(gop), "-keyint_min", strconv.Itoa(gop),
		"-pix_fmt", "yuv420p",
		"-map", "0:v:0", "-map", "0:a?", "-c:a", "aac", "-b:a", fmt.Sprintf("%dk", akbps), "-ac", "2", "-ar", "48000",
		"-map_metadata", "-1", "-map_chapters", "-1",
		"-pass", "2", "-passlogfile", passlog,
	}
	if format == "mp4" {
		args2 = append(args2, "-movflags", "+faststart")
	}
	args2 = append(args2, out)
	return exec.CommandContext(ctx, "ffmpeg", args2...).Run()
}

/* ---------------------- I/O + ZIP + Redis helpers ---------------------- */

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

	baseName := filepath.Base(fr.Result.FilePath)
	ext := filepath.Ext(baseName)
	if ext == "" {
		ext = ".mp4"
	}
	dst := filepath.Join(dir, fmt.Sprintf("src-%s%s", newULID(), ext))

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

func shortHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil))[:8], nil
}
func fileSize(p string) (int64, error) {
	info, err := os.Stat(p)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
func substr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

/* ---------------------- Redis/session keys & helpers ---------------------- */

func keyQuota(user int64, yyyymmdd string) string { return fmt.Sprintf("quota:%d:%s", user, yyyymmdd) }
func keySessionMeta(sid string) string            { return "sess:" + sid + ":meta" }
func keySessionProg(sid string) string            { return "sess:" + sid + ":prog" }
func keySessionOuts(sid string) string            { return "sess:" + sid + ":outs" }

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
		return 0, 0, 0, e
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
