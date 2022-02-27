package dispatcher

import (
	"sync"
	"testing"

	product_sdk "github.com/BrobridgeOrg/gravity-sdk/product"
	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	"github.com/d5/tengo/assert"
	"go.uber.org/zap"
)

func TestProductMessageHandler(t *testing.T) {

	logger = zap.NewExample()
	var wg sync.WaitGroup

	testData := MessageRawData{
		Event:      "dataCreated",
		RawPayload: []byte(`{"id":101,"name":"fred"}`),
	}

	// Preparing processor
	p := NewProcessor(
		WithOutputHandler(func(msg *Message) {
			assert.Equal(t, "dataCreated", msg.Record.EventName)
			assert.Equal(t, "TestDataProduct", msg.Record.Table)

			for _, field := range msg.Record.Fields {
				switch field.Name {
				case "id":
					assert.Equal(t, int64(101), gravity_sdk_types_record.GetValue(field.Value))
				case "name":
					assert.Equal(t, "fred", gravity_sdk_types_record.GetValue(field.Value))
				}
			}

			wg.Done()
		}),
	)

	// Product schema
	productSchemaSource := `
	"id": { "type": "uint" },
	"name": { "type": "string" },
	"type": { "type": "string" },
	"phone": { "type": "string" },
	"address": { "type": "string" }
`

	var productSchema map[string]interface{}
	json.Unmarshal([]byte(productSchemaSource), &productSchema)

	// Preparing product setting
	setting := &product_sdk.ProductSetting{
		Name:        "TestProduct",
		Description: "Product description",
		Enabled:     false,
		Schema:      productSchema,
	}

	// Preapring rule
	r := product_sdk.NewRule()
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

	setting.Rules = map[string]*product_sdk.Rule{
		"testRule": r,
	}

	// Create product and apply setting
	product := NewProduct(nil)
	product.ApplySettings(setting)
	product.Subscribe(func(eventName string, msg *Message) {
		p.Push(msg)
	})

	// Message
	wg.Add(1)
	raw, _ := json.Marshal(testData)
	product.HandleRawMessage(testData.Event, raw)

	wg.Wait()
}

func TestProductTransformerSrcipt(t *testing.T) {

	logger = zap.NewExample()
	var wg sync.WaitGroup

	testData := MessageRawData{
		Event:      "dataCreated",
		RawPayload: []byte(`{"id":101,"name":"fred"}`),
	}

	// Preparing processor
	p := NewProcessor(
		WithOutputHandler(func(msg *Message) {
			assert.Equal(t, "dataCreated", msg.Record.EventName)
			assert.Equal(t, "TestDataProduct", msg.Record.Table)

			for _, field := range msg.Record.Fields {
				switch field.Name {
				case "id":
					assert.Equal(t, int64(101), gravity_sdk_types_record.GetValue(field.Value))
				case "name":
					assert.Equal(t, "fredX", gravity_sdk_types_record.GetValue(field.Value))
				}
			}

			wg.Done()
		}),
	)

	// Product schema
	productSchemaSource := `
	"id": { "type": "uint" },
	"name": { "type": "string" },
	"type": { "type": "string" },
	"phone": { "type": "string" },
	"address": { "type": "string" }
`

	var productSchema map[string]interface{}
	json.Unmarshal([]byte(productSchemaSource), &productSchema)

	// Preparing product setting
	setting := &product_sdk.ProductSetting{
		Name:        "TestProduct",
		Description: "Product description",
		Enabled:     false,
		Schema:      productSchema,
	}

	// Preapring rule
	r := product_sdk.NewRule()
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
	product.ApplySettings(setting)
	product.Subscribe(func(eventName string, msg *Message) {
		p.Push(msg)
	})

	// Message
	wg.Add(1)
	raw, _ := json.Marshal(testData)
	product.HandleRawMessage(testData.Event, raw)

	wg.Wait()
}
