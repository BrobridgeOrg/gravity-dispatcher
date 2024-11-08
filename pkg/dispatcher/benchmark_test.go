package dispatcher

import (
	"testing"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/rule_manager"
	product_sdk "github.com/BrobridgeOrg/gravity-sdk/v2/product"
)

func CreateBenchmarkRule() *rule_manager.Rule {

	r := rule_manager.NewRule(product_sdk.NewRule())
	r.Event = "Benchmark"
	r.Product = "BenchmarkProduct"
	r.PrimaryKey = []string{
		"id",
	}

	r.SchemaConfig = map[string]interface{}{
		"id": map[string]interface{}{
			"type": "int",
		},
		"name": map[string]interface{}{
			"type": "string",
		},
	}

	// initializing rule
	testRuleManager := rule_manager.NewRuleManager()
	testRuleManager.AddRule(r)

	return r
}

func BenchmarkProcessor(b *testing.B) {

	rawPayload := map[string]interface{}{
		"id":   1,
		"name": "benchmark",
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
	msg.Rule = CreateBenchmarkRule()

	p := NewProcessor()
	defer p.Close()

	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		p.process(msg)
		msg.Reset()
	}
}
