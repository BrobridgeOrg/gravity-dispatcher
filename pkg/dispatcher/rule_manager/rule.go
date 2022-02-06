package rule_manager

import "github.com/BrobridgeOrg/schemer"

type Rule struct {
	ID            string                 `json:"id"`
	Event         string                 `json:"event"`
	DataProduct   string                 `json:"dataProduct"`
	Method        string                 `json:"method"`
	PrimaryKey    []string               `json:"primaryKey"`
	SchemaConfig  map[string]interface{} `json:"schema,omitempty"`
	HandlerConfig *HandlerConfig         `json:"handler,omitempty"`
	Handler       *Handler
	Schema        *schemer.Schema
}

func NewRule() *Rule {
	return &Rule{}
}

func (r *Rule) applyConfigs() error {

	// Preparing schema
	schema := schemer.NewSchema()
	err := schemer.Unmarshal(r.SchemaConfig, schema)
	if err != nil {
		return err
	}

	r.Schema = schema

	// Preparing handler
	if r.HandlerConfig == nil {
		r.HandlerConfig = &HandlerConfig{
			Type:   "script",
			Script: "return source",
		}
	}

	r.Handler = NewHandler(r.HandlerConfig)
	r.Handler.Transformer.SetSourceSchema(r.Schema)

	return nil
}
