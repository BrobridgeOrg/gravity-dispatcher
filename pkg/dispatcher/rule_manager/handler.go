package rule_manager

import (
	"github.com/BrobridgeOrg/schemer"
)

type HandlerType int32

const (
	HANDLER_SCRIPT HandlerType = iota
)

var HandlerTypes = map[string]HandlerType{
	"script": HANDLER_SCRIPT,
}

type HandlerConfig struct {
	Type   string `json:"type"`
	Script string `json:"script"`
}

type Handler struct {
	Type        HandlerType
	Script      string
	Transformer *schemer.Transformer
}

func NewHandler(config *HandlerConfig) *Handler {

	handler := &Handler{
		Type: HandlerTypes["script"],
	}

	handler.Transformer = schemer.NewTransformer(nil, nil)

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
