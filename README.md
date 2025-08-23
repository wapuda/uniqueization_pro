# tg-unifier (Go)
Telegram bot + worker that unifies videos/images (strip metadata, transcode, rename).
- Bot enqueues jobs to Redis (Asynq)
- Worker downloads media, runs FFmpeg/ExifTool, and replies with result
