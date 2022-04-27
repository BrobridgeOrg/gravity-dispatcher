package core

import "time"

type Rule struct {
	ID            string                 `json:"id"`
	Name          string                 `json:"name"`
	Description   string                 `json:"desc"`
	Event         string                 `json:"event"`
	Product       string                 `json:"product"`
	Method        string                 `json:"method"`
	PrimaryKey    []string               `json:"primaryKey"`
	SchemaConfig  map[string]interface{} `json:"schema,omitempty"`
	HandlerConfig *HandlerConfig         `json:"handler,omitempty"`
	Enabled       bool                   `json:"enabled"`
	CreatedAt     time.Time              `json:"createdAt"`
	UpdatedAt     time.Time              `json:"updatedAt"`
}

func NewRule() *Rule {
	return &Rule{}
}

type HandlerConfig struct {
	Type   string `json:"type"`
	Script string `json:"script"`
}
