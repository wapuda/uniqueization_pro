package main

import (
	"context"
	"fmt"
	"os"

	"github.com/wapuda/uniqueization_pro/internal/jobs"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run ./cmd/localtest <input.mp4> <amount>")
		return
	}
	in := os.Args[1]
	amount := 0
	fmt.Sscanf(os.Args[2], "%d", &amount)

	payload := jobs.EncodeVariantPayload{
		SessionID:   "localtest",
		Format:      "mp4",
		SrcPath:     in,
		SrcBaseName: in,
		// You can probe actual duration & size here, or use dummy values.
		DurationSec: 30,
		OrigBytes:   5000000,
	}

	for i := 1; i <= amount; i++ {
		out, err := encodeAdaptive(context.Background(), payload, "./out", payload.OrigBytes*2, loadCfg())
		if err != nil {
			panic(err)
		}
		fmt.Println("Generated:", out)
	}
}
