package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/hibiken/asynq"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"github.com/wapuda/uniqueization_pro/internal/logx"

	"github.com/wapuda/uniqueization_pro/internal/jobs"
)

type cfg struct {	
	RedisAddr       string
	PerVideoMax     int
	DailyMax        int
	MaxVideosPerMsg int
}

func mustEnvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func loadCfg() cfg {
	return cfg{
		RedisAddr:       env("REDIS_ADDR", "localhost:6379"),
		PerVideoMax:     mustEnvInt("PER_VIDEO_MAX", 50),
		DailyMax:        mustEnvInt("DAILY_MAX", 200),
		MaxVideosPerMsg: mustEnvInt("MAX_VIDEOS_PER_MSG", 10),
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var (
	rctx = context.Background()
)

// --- Simple in-memory media-group aggregator (2s debounce) ---

type groupState struct {
	userID int64
	mgid   string
	items  []jobs.MediaItem
	timer  *time.Timer
}

type server struct {
	cfg   cfg
	bot   *tgbotapi.BotAPI
	rdb   *redis.Client
	asynq *asynq.Client

	gmu    sync.Mutex
	groups map[string]*groupState // key: userID:mediaGroupID
}

func main() {
	_ = godotenv.Load()
	c := loadCfg()

	logx.Setup(logx.FromEnv("bot"))
	log.Info().Msg("bot starting")

	token := os.Getenv("BOT_TOKEN")
	if token == "" {
		log.Fatal("BOT_TOKEN is required")
	}

	// health endpoint
	go func() {
		http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"ok":true}`)) })
		log.Println("bot health on :8080/health")
		log.Println(http.ListenAndServe(":8080", nil))
	}()

	bot, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		log.Fatal(err)
	}
	bot.Debug = false

	// on successful auth:
	log.Info().Str("username", bot.Self.UserName).Msg("bot authorized")

	rdb := redis.NewClient(&redis.Options{Addr: c.RedisAddr})
	asClient := asynq.NewClient(asynq.RedisClientOpt{Addr: c.RedisAddr})

	s := &server{
		cfg:    c,
		bot:    bot,
		rdb:    rdb,
		asynq:  asClient,
		groups: make(map[string]*groupState),
	}

	log.Printf("Bot authorized as @%s", bot.Self.UserName)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 30
	updates := bot.GetUpdatesChan(u)

	for upd := range updates {
		switch {
		case upd.Message != nil:
			s.onMessage(upd.Message)
		case upd.CallbackQuery != nil:
			s.onCallback(upd.CallbackQuery)
		}
	}
}

// --- Handlers ---

func (s *server) onMessage(m *tgbotapi.Message) {
	// Log key UI events
	log.Info().
		Int64("chat_id", m.Chat.ID).
		Int64("user_id", m.From.ID).
		Msg("message received")

	// Commands
	if m.IsCommand() {
		switch m.Command() {
		case "start":
			s.resetState(m.From.ID)
			rem := s.remainingToday(m.From.ID)
			msg := "Send 1–10 videos (album supported).\n" +
				fmt.Sprintf("Then choose format and amount (1–%d per video).\nDaily cap: %d outputs. Remaining today: %d.", s.cfg.PerVideoMax, s.cfg.DailyMax, rem)
			_, _ = s.bot.Send(tgbotapi.NewMessage(m.Chat.ID, msg))
		case "cancel":
			s.resetState(m.From.ID)
			_, _ = s.bot.Send(tgbotapi.NewMessage(m.Chat.ID, "Session canceled. Send videos to start again."))
		default:
			_, _ = s.bot.Send(tgbotapi.NewMessage(m.Chat.ID, "Unknown command. Send videos to start."))
		}
		return
	}

	// Amount entry flow
	if m.Text != "" && s.getState(m.From.ID) == "awaiting_amount" {
		s.handleAmountInput(m)
		return
	}

	// Media intake (videos only)
	item, ok := extractVideoAsItem(m)
	if !ok {
		// Ignore non-video messages unless we are waiting for amount
		return
	}

	// media group?
	if m.MediaGroupID != "" {
		s.addToGroup(m.From.ID, m.MediaGroupID, item, m.Chat.ID)
		return
	}

	// single video
	if err := s.setPending(m.From.ID, []jobs.MediaItem{item}); err != nil {
		_, _ = s.bot.Send(tgbotapi.NewMessage(m.Chat.ID, "Internal error (pending). Try again."))
		return
	}
	
	// Log key UI events - single video received
	log.Info().Int("count", 1).Msg("videos collected; asking for format")
	
	s.setState(m.From.ID, "awaiting_format")
	s.askFormat(m.Chat.ID)
}

func (s *server) onCallback(cq *tgbotapi.CallbackQuery) {
	data := cq.Data
	userID := cq.From.ID
	chatID := cq.Message.Chat.ID

	if strings.HasPrefix(data, "fmt:") {
		format := strings.TrimPrefix(data, "fmt:")
		if format != "mp4" && format != "mov" {
			_, _ = s.bot.Send(tgbotapi.NewMessage(chatID, "Select one format: MP4 or MOV."))
			return
		}

		if s.getState(userID) != "awaiting_format" {
			_ = s.answerCB(cq, "No active session. Send videos first.")
			return
		}
		if err := s.rdb.Set(rctx, keyFormat(userID), format, 24*time.Hour).Err(); err != nil {
			_ = s.answerCB(cq, "Internal error")
			return
		}
		_ = s.answerCB(cq, "Format selected: "+strings.ToUpper(format))

		// Log key UI events - format button tapped
		log.Info().
			Int64("user_id", cq.From.ID).
			Str("format", format).
			Msg("format selected")

		s.setState(userID, "awaiting_amount")
		rem := s.remainingToday(userID)
		cnt := s.pendingCount(userID)
		txt := fmt.Sprintf("Format: %s ✅\nNow enter amount (1–%d) for each video.\nYou sent %d video(s). Remaining today: %d outputs.",
			strings.ToUpper(format), s.cfg.PerVideoMax, cnt, rem)
		_, _ = s.bot.Send(tgbotapi.NewMessage(chatID, txt))
		return
	}
	_ = s.answerCB(cq, "")
}

func (s *server) handleAmountInput(m *tgbotapi.Message) {
	userID := m.From.ID
	chatID := m.Chat.ID

	amt, err := strconv.Atoi(strings.TrimSpace(m.Text))
	if err != nil || amt < 1 || amt > s.cfg.PerVideoMax {
		_, _ = s.bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("❌ Amount invalid. Enter a number 1–%d.", s.cfg.PerVideoMax)))
		return
	}

	items, err := s.getPending(userID)
	if err != nil || len(items) == 0 {
		_, _ = s.bot.Send(tgbotapi.NewMessage(chatID, "No pending videos. Send videos again."))
		return
	}

	planned := len(items) * amt
	remain := s.remainingToday(userID)
	if planned > remain || planned > s.cfg.DailyMax {
		_, _ = s.bot.Send(tgbotapi.NewMessage(chatID,
			fmt.Sprintf("❌ Over daily limit. You planned %d outputs but have %d remaining today (cap %d). Enter a smaller amount.",
				planned, remain, s.cfg.DailyMax)))
		return
	}

	format, _ := s.rdb.Get(rctx, keyFormat(userID)).Result()
	if format != "mp4" && format != "mov" {
		_, _ = s.bot.Send(tgbotapi.NewMessage(chatID, "Select format first (MP4/MOV)."))
		return
	}

	// Log key UI events - amount parsed and validated
	log.Info().
		Int("amount", amt).
		Int("videos", len(items)).
		Int("planned_outputs", planned).
		Int("remaining_today", remain).
		Msg("amount accepted; enqueueing session")

	// Enqueue session:start
	sid := "" // worker will generate if empty
	payload := jobs.StartSessionPayload{
		SessionID: sid,
		ChatID:    chatID,
		UserID:    userID,
		Format:    format,
		Amount:    amt,
		Items:     items,
	}
	b, _ := json.Marshal(payload)
	_, err = s.asynq.EnqueueContext(rctx, asynq.NewTask(jobs.TaskStartSession, b), asynq.MaxRetry(5))
	if err != nil {
		// Log enqueue failure
		log.Error().Err(err).Msg("asynq enqueue session:start failed")
		_, _ = s.bot.Send(tgbotapi.NewMessage(chatID, "Queue error: "+err.Error()))
		return
	}

	// Reset state and confirm
	s.resetState(userID)
	_, _ = s.bot.Send(tgbotapi.NewMessage(chatID,
		fmt.Sprintf("Queued ✅  You will receive a ZIP with %d unique video(s).", planned)))
}

// --- Media helpers ---

func extractVideoAsItem(m *tgbotapi.Message) (jobs.MediaItem, bool) {
	// Prefer real videos
	if m.Video != nil {
		return jobs.MediaItem{FileID: m.Video.FileID, Name: ""}, true
	}
	// Accept video as document if mime suggests video/*
	if m.Document != nil && strings.HasPrefix(strings.ToLower(m.Document.MimeType), "video/") {
		return jobs.MediaItem{FileID: m.Document.FileID, Name: m.Document.FileName}, true
	}
	return jobs.MediaItem{}, false
}

// --- Group aggregation (2s debounce) ---

func (s *server) addToGroup(userID int64, mgid string, it jobs.MediaItem, chatID int64) {
	key := fmt.Sprintf("%d:%s", userID, mgid)

	s.gmu.Lock()
	defer s.gmu.Unlock()

	g, ok := s.groups[key]
	if !ok {
		g = &groupState{userID: userID, mgid: mgid}
		s.groups[key] = g
	}
	// Append item if we still have room
	if len(g.items) < s.cfg.MaxVideosPerMsg {
		g.items = append(g.items, it)
	}

	// (Re)start debounce timer
	if g.timer != nil {
		g.timer.Stop()
	}
	g.timer = time.AfterFunc(2*time.Second, func() {
		s.finalizeGroup(userID, mgid, chatID)
	})
}

func (s *server) finalizeGroup(userID int64, mgid string, chatID int64) {
	key := fmt.Sprintf("%d:%s", userID, mgid)

	s.gmu.Lock()
	g, ok := s.groups[key]
	if ok {
		delete(s.groups, key)
	}
	s.gmu.Unlock()
	if !ok {
		return
	}

	// Trim to max
	items := g.items
	if len(items) == 0 {
		return
	}
	if len(items) > s.cfg.MaxVideosPerMsg {
		items = items[:s.cfg.MaxVideosPerMsg]
	}

	if err := s.setPending(userID, items); err != nil {
		_, _ = s.bot.Send(tgbotapi.NewMessage(chatID, "Internal error (pending). Try again."))
		return
	}
	
	// Log key UI events - videos collected; asking for format
	log.Info().Int("count", len(items)).Msg("videos collected; asking for format")
	
	s.setState(userID, "awaiting_format")
	s.askFormat(chatID)
}

func (s *server) askFormat(chatID int64) {
	btns := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("MP4", "fmt:mp4"),
			tgbotapi.NewInlineKeyboardButtonData("MOV", "fmt:mov"),
		),
	)
	msg := tgbotapi.NewMessage(chatID, "Choose output format:")
	msg.ReplyMarkup = btns
	_, _ = s.bot.Send(msg)
}

// --- State persistence (Redis) ---

func keyPending(user int64) string { return fmt.Sprintf("pending:%d", user) }
func keyState(user int64) string   { return fmt.Sprintf("state:%d", user) }
func keyFormat(user int64) string  { return fmt.Sprintf("format:%d", user) }
func keyQuota(user int64, ymd string) string {
	return fmt.Sprintf("quota:%d:%s", user, ymd)
}

func (s *server) setPending(user int64, items []jobs.MediaItem) error {
	b, _ := json.Marshal(items)
	return s.rdb.Set(rctx, keyPending(user), string(b), 24*time.Hour).Err()
}
func (s *server) getPending(user int64) ([]jobs.MediaItem, error) {
	raw, err := s.rdb.Get(rctx, keyPending(user)).Result()
	if err != nil {
		return nil, err
	}
	var items []jobs.MediaItem
	_ = json.Unmarshal([]byte(raw), &items)
	return items, nil
}
func (s *server) pendingCount(user int64) int {
	items, err := s.getPending(user)
	if err != nil {
		return 0
	}
	return len(items)
}

func (s *server) setState(user int64, st string) {
	_ = s.rdb.Set(rctx, keyState(user), st, 24*time.Hour).Err()
}
func (s *server) getState(user int64) string {
	st, _ := s.rdb.Get(rctx, keyState(user)).Result()
	return st
}

func (s *server) resetState(user int64) {
	_ = s.rdb.Del(rctx, keyPending(user), keyState(user), keyFormat(user)).Err()
}

func today() string { return time.Now().Format("20060102") }
func untilMidnight() time.Duration {
	now := time.Now()
	tom := now.Add(24 * time.Hour)
	mid := time.Date(tom.Year(), tom.Month(), tom.Day(), 0, 0, 0, 0, now.Location())
	return time.Until(mid)
}

// Remaining outputs today (not authoritative—final charge done by Worker)
func (s *server) remainingToday(user int64) int {
	key := keyQuota(user, today())
	used, _ := s.rdb.Get(rctx, key).Int()
	if used < 0 {
		used = 0
	}
	rem := s.cfg.DailyMax - used
	if rem < 0 {
		rem = 0
	}
	// Ensure TTL is set (nice-to-have)
	_ = s.rdb.Expire(rctx, key, untilMidnight()).Err()
	return rem
}

func (s *server) answerCB(cq *tgbotapi.CallbackQuery, text string) error {
	_, err := s.bot.Request(tgbotapi.NewCallback(cq.ID, text))
	return err
}
