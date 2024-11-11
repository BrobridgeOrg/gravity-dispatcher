package rule_manager

import (
	"encoding/json"
	"testing"

	product_sdk "github.com/BrobridgeOrg/gravity-sdk/v2/product"
	"github.com/BrobridgeOrg/schemer"
)

func BenchmarkHandler(b *testing.B) {

	// Product schema
	schemaSource := `{
	"id": { "type": "int" },
	"name": { "type": "string" },
	"type": { "type": "string" },
	"phone": { "type": "string" },
	"address": { "type": "string" }
}`

	var schemaMap map[string]interface{}
	json.Unmarshal([]byte(schemaSource), &schemaMap)

	schema := schemer.NewSchema()
	schemer.Unmarshal(schemaMap, schema)

	config := &product_sdk.HandlerConfig{}
	config.Type = "script"
	config.Script = `return source`

	h := NewHandler(config, schema, schema)

	sample := []byte(`{"id":101,"name":"fred","type":"customer","phone":"1234567890","address":"1234 Main St"}`)
	var data map[string]interface{}
	json.Unmarshal(sample, &data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Run(nil, data)
	}
}
