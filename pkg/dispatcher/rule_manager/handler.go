package rule_manager

import (
	product_sdk "github.com/BrobridgeOrg/gravity-sdk/v2/product"
	"github.com/BrobridgeOrg/schemer"
	goja_runtime "github.com/BrobridgeOrg/schemer/runtime/goja"
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

	handler.Transformer = schemer.NewTransformer(
		sourceSchema,
		targetSchema,
		schemer.WithRuntime(goja_runtime.NewRuntime()),
	)

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

func (e *Handler) GetDestinationSchema() *schemer.Schema {
	return e.Transformer.GetDestinationSchema()
}
