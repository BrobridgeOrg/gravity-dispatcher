package dispatcher

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/message"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/rule_manager"
	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	"github.com/d5/tengo/assert"
	"go.uber.org/zap"
)

var testRuleMaanager = rule_manager.NewRuleManager()

func CreateTestRule() *rule_manager.Rule {

	r := rule_manager.NewRule()
	r.Event = "dataCreated"
	r.Collection = "TestCollection"
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
	testRuleMaanager.AddRule(r)

	return r
}

func TestProcessorOutput(t *testing.T) {

	logger = zap.NewExample()

	done := make(chan struct{})

	p := NewProcessor(
		WithOutputHandler(func(msg *message.Message) {
			assert.Equal(t, "dataCreated", msg.Record.EventName)
			assert.Equal(t, "TestCollection", msg.Record.Table)

			for _, field := range msg.Record.Fields {
				switch field.Name {
				case "id":
					assert.Equal(t, int64(101), gravity_sdk_types_record.GetValue(field.Value))
				case "name":
					assert.Equal(t, "fred", gravity_sdk_types_record.GetValue(field.Value))
				}
			}

			done <- struct{}{}
		}),
	)

	testData := map[string]interface{}{
		"event":   "dataCreated",
		"payload": `{"id":101,"name":"fred"}`,
	}

	// Create a new message
	msg := message.New()
	msg.Rule = CreateTestRule()

	// Preparing raw data
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
		WithOutputHandler(func(msg *message.Message) {
			assert.Equal(t, "dataCreated", msg.Record.EventName)
			assert.Equal(t, "TestCollection", msg.Record.Table)

			count++

			for _, field := range msg.Record.Fields {
				switch field.Name {
				case "id":
					assert.Equal(t, count, gravity_sdk_types_record.GetValue(field.Value))
				case "name":
					assert.Equal(t, "test", gravity_sdk_types_record.GetValue(field.Value))
				}
			}

			wg.Done()
		}),
	)

	wg.Add(100)
	for i := 1; i <= 100; i++ {

		rawPayload := map[string]interface{}{
			"id":   i,
			"name": "test",
		}

		payload, _ := json.Marshal(rawPayload)

		testData := map[string]interface{}{
			"event":   "dataCreated",
			"payload": string(payload),
		}

		// Create a new message
		msg := message.New()
		msg.Rule = CreateTestRule()

		// Preparing raw data
		raw, _ := json.Marshal(testData)
		msg.Raw = raw

		p.Push(msg)
	}

	wg.Done()
}
