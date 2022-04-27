package system

import (
	"fmt"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type Route struct {
	prefix string
	rpc    *RPC
}

func NewRoute(rpc *RPC, prefix string) *Route {
	return &Route{
		rpc:    rpc,
		prefix: prefix,
	}
}

func (r *Route) Handle(apiPath string, h func(*nats.Msg)) {

	uri := fmt.Sprintf("%s.%s", r.prefix, apiPath)

	logger.Info("Registered API",
		zap.String("path", uri),
	)

	conn := r.rpc.connection
	conn.QueueSubscribe(uri, "system", func(m *nats.Msg) {
		logger.Info("-> " + uri)
		h(m)
	})
}
