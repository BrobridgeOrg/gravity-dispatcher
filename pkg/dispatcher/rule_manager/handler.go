package rule_manager

import (
	product_sdk "github.com/BrobridgeOrg/gravity-sdk/v2/product"
	"github.com/BrobridgeOrg/schemer"
)

type HandlerType int32

const (
	HANDLER_SCRIPT HandlerType = iota
)

var HandlerTypes = map[string]HandlerType{
	"script": HANDLER_SCRIPT,
}

type Handler struct {
	Type        HandlerType
	Script      string
	Transformer *schemer.Transformer
}

func NewHandler(config *product_sdk.HandlerConfig, sourceSchema *schemer.Schema, targetSchema *schemer.Schema) *Handler {

	handler := &Handler{
		Type: HandlerTypes["script"],
	}

	handler.Transformer = schemer.NewTransformer(sourceSchema, targetSchema)

	if config != nil {
		if t, ok := HandlerTypes[config.Type]; ok {
			handler.Type = t
		}

		handler.Script = config.Script
		handler.Transformer.SetScript(config.Script)
	}

	return handler
}

func (e *Handler) Run(env map[string]interface{}, data map[string]interface{}) ([]map[string]interface{}, error) {
	return e.Transformer.Transform(env, data)
}
