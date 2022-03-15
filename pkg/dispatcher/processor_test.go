package dispatcher

import (
	"sync"
	"testing"

	record_type "github.com/BrobridgeOrg/compton/types/record"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/rule_manager"
	product_sdk "github.com/BrobridgeOrg/gravity-sdk/product"
	"github.com/d5/tengo/assert"
	"go.uber.org/zap"
)

func CreateTestRule() *rule_manager.Rule {

	r := rule_manager.NewRule(product_sdk.NewRule())
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

func CreateTestMessage() *Message {

	// Prparing rule
	testRuleManager := rule_manager.NewRuleManager()
	r := CreateTestRule()
	testRuleManager.AddRule(r)

	msg := NewMessage()
	msg.Rule = r

	return msg
}

func TestProcessorOutput(t *testing.T) {

	logger = zap.NewExample()

	done := make(chan struct{})

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

			done <- struct{}{}
		}),
	)

	testData := MessageRawData{
		Event:      "dataCreated",
		RawPayload: []byte(`{"id":101,"name":"fred"}`),
	}

	// Preparing message with raw data
	msg := CreateTestMessage()
	raw, _ := json.Marshal(testData)
	msg.Raw = raw

	p.Push(msg)

	<-done
}

func TestProcessorOutputsWithMultipleInputs(t *testing.T) {

	logger = zap.NewExample()

	var wg sync.WaitGroup
	count := int64(0)

	p := NewProcessor(
		WithOutputHandler(func(msg *Message) {
			assert.Equal(t, "dataCreated", msg.ProductEvent.EventName)
			assert.Equal(t, "TestDataProduct", msg.ProductEvent.Table)

			count++

			r, err := msg.ProductEvent.GetContent()
			assert.Equal(t, nil, err)

			for _, field := range r.Payload.Map.Fields {
				switch field.Name {
				case "id":
					assert.Equal(t, count, record_type.GetValueData(field.Value))
				case "name":
					assert.Equal(t, "test", record_type.GetValueData(field.Value))
				}
			}

			wg.Done()
		}),
	)

	num := 1000
	wg.Add(num)
	for i := 1; i <= num; i++ {

		rawPayload := map[string]interface{}{
			"id":   i,
			"name": "test",
		}

		payload, _ := json.Marshal(rawPayload)

		testData := MessageRawData{
			Event:      "dataCreated",
			RawPayload: payload,
		}

		// Preparing message with raw data
		msg := CreateTestMessage()
		raw, _ := json.Marshal(testData)
		msg.Raw = raw

		p.Push(msg)
	}

	wg.Wait()
}
