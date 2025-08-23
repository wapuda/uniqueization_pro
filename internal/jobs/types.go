package jobs

const (
	TaskStartSession   = "session:start"
	TaskEncodeVariant  = "encode:variant"
	TaskFinalize       = "session:finalize"
)

type MediaItem struct {
	FileID string `json:"file_id"` // Telegram file_id
	Name   string `json:"name"`    // optional
}

type StartSessionPayload struct {
	SessionID string      `json:"session_id"` // optional; if empty, worker creates one
	ChatID    int64       `json:"chat_id"`
	UserID    int64       `json:"user_id"`
	Format    string      `json:"format"`     // "mp4" or "mov" (single choice)
	Amount    int         `json:"amount"`     // 1..50 per video
	Items     []MediaItem `json:"items"`      // 1..10 videos
}

type EncodeVariantPayload struct {
	SessionID   string  `json:"session_id"`
	ChatID      int64   `json:"chat_id"`
	UserID      int64   `json:"user_id"`
	Format      string  `json:"format"`      // "mp4" or "mov"
	SrcPath     string  `json:"src_path"`    // local path prepared by StartSession
	SrcBaseName string  `json:"src_base"`    // for captions
	DurationSec float64 `json:"duration_s"`
	OrigBytes   int64   `json:"orig_bytes"`
	Index       int     `json:"index"`       // running number
	Total       int     `json:"total"`       // total variants in session (for progress)
}

type FinalizePayload struct {
	SessionID string `json:"session_id"`
	ChatID    int64  `json:"chat_id"`
	UserID    int64  `json:"user_id"`
	Total     int    `json:"total"`
}
