package dispatcher

import (
	"testing"

	"go.uber.org/zap"
)

func BenchmarkProcessor(b *testing.B) {

	logger = zap.NewExample()

	rawPayload := map[string]interface{}{
		"id":   1,
		"name": "test",
	}

	payload, _ := json.Marshal(rawPayload)

	testData := MessageRawData{
		Event:      "Benchmark",
		RawPayload: payload,
	}

	// Preparing message with raw data
	raw, _ := json.Marshal(testData)
	msg := CreateTestMessage()
	msg.Raw = raw

	p := NewProcessor()

	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		p.handle(msg, func(interface{}) {})
		msg.Reset()
	}
}
