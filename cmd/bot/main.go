package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/hibiken/asynq"
	"github.com/joho/godotenv"
	"github.com/you/tg-unifier/internal/jobs"
)

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" { return v }
	return def
}

func main() {
	_ = godotenv.Load()
	botToken := os.Getenv("BOT_TOKEN")
	if botToken == "" { log.Fatal("BOT_TOKEN is required") }
	redisAddr := getenv("REDIS_ADDR", "localhost:6379")

	// health endpoint
	go func() {
		http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request){ w.Write([]byte(`{"ok":true}`)) })
		log.Println("bot health on :8080/health")
		log.Println(http.ListenAndServe(":8080", nil))
	}()

	bot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil { log.Fatal(err) }
	bot.Debug = false

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 30
	updates := bot.GetUpdatesChan(u)

	client := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr})
	defer client.Close()

	log.Printf("Bot authorized as @%s", bot.Self.UserName)

	for upd := range updates {
		if upd.Message == nil { continue }

		var fileID, kind string
		if v := upd.Message.Video; v != nil {
			fileID, kind = v.FileID, "video"
		} else if p := upd.Message.Photo; len(p) > 0 {
			fileID, kind = p[len(p)-1].FileID, "photo"
		} else if d := upd.Message.Document; d != nil {
			fileID, kind = d.FileID, "document"
		} else {
			continue
		}

		payload := jobs.UnifyPayload{
			ChatID: upd.Message.Chat.ID,
			FileID: fileID,
			Kind:   kind,
		}
		b, _ := json.Marshal(payload)
		task := asynq.NewTask(jobs.TaskUnify, b)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := client.EnqueueContext(ctx, task, asynq.MaxRetry(5), asynq.Retention(24*time.Hour))
		cancel()
		if err != nil {
			bot.Send(tgbotapi.NewMessage(upd.Message.Chat.ID, "Queue error: "+err.Error()))
			continue
		}
		bot.Send(tgbotapi.NewMessage(upd.Message.Chat.ID, "Got it! Processing…"))
	}
}
