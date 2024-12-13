package dispatcher

import (
	"sync"
	"testing"

	product_sdk "github.com/BrobridgeOrg/gravity-sdk/v2/product"
	record_type "github.com/BrobridgeOrg/gravity-sdk/v2/types/record"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func CreateTestProductSetting() *product_sdk.ProductSetting {

	// Product schema
	productSchemaSource := `{
	"id": { "type": "int" },
	"name": { "type": "string" },
	"type": { "type": "string" },
	"phone": { "type": "string" },
	"address": { "type": "string" }
}`

	var productSchema map[string]interface{}
	json.Unmarshal([]byte(productSchemaSource), &productSchema)

	// Preparing product setting
	setting := &product_sdk.ProductSetting{
		Name:        "TestProduct",
		Description: "Product description",
		Enabled:     false,
		Schema:      productSchema,
	}

	return setting
}

func CreateTestProductRule() *product_sdk.Rule {

	r := product_sdk.NewRule()
	r.Name = "test_rule"
	r.Event = "dataCreated"
	r.Product = "TestDataProduct"
	r.PrimaryKey = []string{
		"id",
	}

	schemaRaw := `{
	"id": { "type": "int" },
	"name": { "type": "string" }
}`

	var schemaConfig map[string]interface{}
	json.Unmarshal([]byte(schemaRaw), &schemaConfig)

	r.SchemaConfig = schemaConfig

	return r
}

func TestProductMessageHandler(t *testing.T) {

	logger = zap.NewNop()
	var wg sync.WaitGroup

	testData := MessageRawData{
		Event:      "dataCreated",
		RawPayload: []byte(`{"id":101,"name":"fred"}`),
	}

	// Preparing processor
	p := NewProcessor(
		WithOutputHandler(func(msg *Message) {
			assert.Equal(t, "dataCreated", msg.ProductEvent.EventName)
			assert.Equal(t, "TestDataProduct", msg.ProductEvent.Table)

			r, err := msg.ProductEvent.GetContent()
			assert.Equal(t, nil, err)

			for _, field := range r.Payload.Map.Fields {
				switch field.Name {
				case "id":
					assert.Equal(t, int64(101), record_type.GetValueData(field.Value))
				case "name":
					assert.Equal(t, "fred", record_type.GetValueData(field.Value))
				}
			}

			wg.Done()
		}),
	)

	// Preparing product
	setting := CreateTestProductSetting()

	// Preapring rule
	r := CreateTestProductRule()

	setting.Rules = map[string]*product_sdk.Rule{
		"testRule": r,
	}

	// Create product and apply setting
	product := NewProduct(nil)
	product.onMessage = func(msg *Message) {
		p.Push(msg)
	}
	product.ApplySettings(setting)

	// Message
	wg.Add(1)
	raw, _ := json.Marshal(testData)
	product.HandleRawMessage(testData.Event, raw)

	wg.Wait()
}

func TestProductTransformerSrcipt(t *testing.T) {

	logger = zap.NewNop()
	var wg sync.WaitGroup

	testData := MessageRawData{
		Event:      "dataCreated",
		RawPayload: []byte(`{"id":101,"name":"fred"}`),
	}

	// Preparing processor
	p := NewProcessor(
		WithOutputHandler(func(msg *Message) {
			assert.Equal(t, "dataCreated", msg.ProductEvent.EventName)
			assert.Equal(t, "TestDataProduct", msg.ProductEvent.Table)

			r, err := msg.ProductEvent.GetContent()
			assert.Equal(t, nil, err)

			for _, field := range r.Payload.Map.Fields {
				switch field.Name {
				case "id":
					assert.Equal(t, int64(101), record_type.GetValueData(field.Value))
				case "name":
					assert.Equal(t, "fredX", record_type.GetValueData(field.Value))
				}
			}

			wg.Done()
		}),
	)

	// Preparing product
	setting := CreateTestProductSetting()

	// Preapring rule
	r := CreateTestProductRule()
	r.HandlerConfig = &product_sdk.HandlerConfig{
		Type: "script",
		Script: `
		return {
			id: source.id,
			name: source.name + 'X'
		}
		`,
	}

	setting.Rules = map[string]*product_sdk.Rule{
		"testRule": r,
	}

	// Create product and apply setting
	product := NewProduct(nil)
	product.onMessage = func(msg *Message) {
		p.Push(msg)
	}
	product.ApplySettings(setting)

	// Message
	wg.Add(1)
	raw, _ := json.Marshal(testData)
	product.HandleRawMessage(testData.Event, raw)

	wg.Wait()
}

func TestProductMessageHandler_StressTest(t *testing.T) {

	logger = zap.NewNop()

	targetNum := 100000

	results := make(chan *record_type.Record, targetNum)
	defer close(results)

	// Preparing processor
	p := NewProcessor(
		WithOutputHandler(func(msg *Message) {
			r, err := msg.ProductEvent.GetContent()
			if !assert.Nil(t, err) {
				return
			}

			results <- r
		}),
	)
	defer p.Close()

	// Preparing product
	setting := CreateTestProductSetting()

	// Preapring rule
	r := CreateTestProductRule()

	setting.Rules = map[string]*product_sdk.Rule{
		"testRule": r,
	}

	// Create product and apply setting
	product := NewProduct(nil)
	product.onMessage = func(msg *Message) {
		p.Push(msg)
	}
	product.ApplySettings(setting)

	// Prepare Messages
	payload := map[string]interface{}{
		"id": 0,
	}

	go func() {
		for i := 0; i < targetNum; i++ {

			id := i + 1
			payload["id"] = id
			payloadBytes, _ := json.Marshal(payload)

			testData := MessageRawData{
				Event:      "dataCreated",
				RawPayload: payloadBytes,
			}

			raw, _ := json.Marshal(testData)
			product.HandleRawMessage(testData.Event, raw)

			if id%10000 == 0 {
				t.Log("Processed", id)
			}
		}
	}()

	counter := 0
	for r := range results {
		counter++
		for _, field := range r.Payload.Map.Fields {
			switch field.Name {
			case "id":
				if !assert.Equal(t, int64(counter), record_type.GetValueData(field.Value)) {
					return
				}
			}
		}

		if counter == targetNum {
			break
		}
	}

	assert.Equal(t, counter, targetNum)
}
