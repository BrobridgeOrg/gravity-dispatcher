package rule_manager

import (
	"sync"

	product_sdk "github.com/BrobridgeOrg/gravity-sdk/v2/product"
	"github.com/BrobridgeOrg/schemer"
)

type Rule struct {
	product_sdk.Rule
	handlerPool  sync.Pool
	Handler      *Handler
	Schema       *schemer.Schema
	TargetSchema *schemer.Schema
}

func NewRule(rule *product_sdk.Rule) *Rule {

	r := &Rule{
		Rule: *rule,
	}

	r.handlerPool = sync.Pool{
		New: func() interface{} {
			return NewHandler(r.HandlerConfig, r.Schema, r.TargetSchema)
		},
	}

	return r
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
	r.handlerPool.Put(r.Handler)

	return nil
}

func (r *Rule) Transform(env map[string]interface{}, data map[string]interface{}) ([]map[string]interface{}, error) {
	handler := r.handlerPool.Get()
	defer r.handlerPool.Put(handler)
	return handler.(*Handler).Run(env, data)
}
