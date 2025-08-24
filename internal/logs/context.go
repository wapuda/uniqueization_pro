package logx

type ctxKey string

const (
	CtxKeySessionID ctxKey = "sid"
	CtxKeyUserID    ctxKey = "uid"
)
