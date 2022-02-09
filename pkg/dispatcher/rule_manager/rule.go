package rule_manager

import (
	product_sdk "github.com/BrobridgeOrg/gravity-sdk/product"
	"github.com/BrobridgeOrg/schemer"
)

type Rule struct {
	product_sdk.Rule
	Handler      *Handler
	Schema       *schemer.Schema
	TargetSchema *schemer.Schema
}

func NewRule(rule *product_sdk.Rule) *Rule {
	return &Rule{
		Rule: *rule,
	}
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
		r.HandlerConfig = &product_sdk.HandlerConfig{
			Type:   "script",
			Script: "return source",
		}
	}

	r.Handler = NewHandler(r.HandlerConfig, r.Schema, r.TargetSchema)

	return nil
}
