package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/hibiken/asynq"
	"github.com/joho/godotenv"
	"github.com/oklog/ulid/v2"
	"github.com/redis/go-redis/v9"

	"github.com/you/tg-unifier/internal/jobs"
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
	srv := asynq.NewServer(asynq.RedisClientOpt{Addr: c.RedisAddr}, asynq.Config{
		Concurrency: c.Concurrency,
	})
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
		return sendAndErr(bot, p.ChatID, fmt.Sprintf("Daily limit is %d. You have %d left for today. Enter a smaller amount.", c.DailyMax, remain))
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
				SessionID: sid, ChatID: p.ChatID, UserID: p.UserID,
				Format: f, SrcPath: srcPath, SrcBaseName: baseName,
				DurationSec: dur, OrigBytes: info.Size(),
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

	// Decide delivery route
	shouldUseFilebin := c.FilebinEnable && (c.FilebinAlways || size > c.TelegramUploadMaxByte)

	if shouldUseFilebin {
		bin := p.SessionID
		if pref := strings.TrimSpace(c.FilebinBinPrefix); pref != "" {
			bin = pref + "-" + p.SessionID
		}
		fileURL, binURL, upErr := uploadToFilebin(ctx, c.FilebinBase, bin, zipPath, zipName)
		if upErr != nil {
			log.Printf("filebin upload failed: %v (size=%d)", upErr, size)
			// Fallback: if small enough, try Telegram; else notify user.
			if size <= c.TelegramUploadMaxByte {
				return sendZipViaTelegram(bot, p.ChatID, zipPath, p.Total, p.SessionID, rdb)
			}
			_, _, mid, _ := getProgress(ctx, rdb, p.SessionID)
			msg := tgbotapi.NewEditMessageText(p.ChatID, mid,
				fmt.Sprintf("Finished %d videos.\nCould not upload ZIP to Filebin: %v", p.Total, upErr))
			_, _ = bot.Send(msg)
			return nil
		}
		// Send link with brief info (Filebin bins auto-expire ~6 days)
		_, _, mid, _ := getProgress(ctx, rdb, p.SessionID)
		text := fmt.Sprintf("Done: %d / %d (100%%).\nDownload your ZIP:\n%s\n(Backup link to bin: %s)\nNote: link expires in ~6 days.", p.Total, p.Total, fileURL, binURL)
		_, _ = bot.Send(tgbotapi.NewEditMessageText(p.ChatID, mid, text))
		_ = chargeDaily(ctx, rdb, c, p.UserID, p.Total)
		return nil
	}

	// Telegram route
	return sendZipViaTelegram(bot, p.ChatID, zipPath, p.Total, p.SessionID, rdb)
}

func sendZipViaTelegram(bot *tgbotapi.BotAPI, chatID int64, zipPath string, total int, sid string, rdb *redis.Client) error {
	doc := tgbotapi.NewDocument(chatID, tgbotapi.FilePath(zipPath))
	doc.Caption = "Your unified videos (ZIP)"
	if _, err := bot.Send(doc); err != nil {
		return err
	}
	_, _, mid, _ := getProgress(context.Background(), rdb, sid)
	final := tgbotapi.NewEditMessageText(chatID, mid, fmt.Sprintf("Done: %d / %d (100%%).", total, total))
	_, _ = bot.Send(final)
	return nil
}
