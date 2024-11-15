package dispatcher

import (
	"testing"

	product_sdk "github.com/BrobridgeOrg/gravity-sdk/v2/product"
	"go.uber.org/zap"
)

func BenchmarkProcessor(b *testing.B) {

	logger = zap.NewNop()

	// Preparing processor
	results := make(chan interface{}, 1024)
	p := NewProcessor(
		WithOutputHandler(func(msg *Message) {
			c, _ := msg.ProductEvent.GetContent()
			msg.Reset()
			results <- c
		}),
	)
	defer p.Close()

	// Preparing product
	setting := CreateTestProductSetting()
	setting.Rules = map[string]*product_sdk.Rule{
		"testRule": CreateTestProductRule(),
	}

	// Create product and apply setting
	product := NewProduct(nil)
	defer product.deactivate()
	product.onMessage = func(msg *Message) {
		// Pushing message to processor
		p.Push(msg)
	}
	product.ApplySettings(setting)

	go func() {

		testData := MessageRawData{
			Event:      "dataCreated",
			RawPayload: []byte(`{"id":101,"name":"fred"}`),
		}

		raw, _ := json.Marshal(testData)

		for i := 0; i < b.N; i++ {
			product.HandleRawMessage(testData.Event, raw)
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		<-results
	}
}
