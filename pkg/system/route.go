package system

import (
	"fmt"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type ContentType int32

const (
	ContentType_Bytes ContentType = iota
	ContentType_JSON
)

type RPCContext struct {
	Req RPCRequest
	Res RPCResponse
}

type RPCRequest struct {
	Header map[string]interface{}
	Data   []byte
}

type RPCResponse struct {
	ContentType ContentType
	Header      nats.Header
	Data        interface{}
	Error       error
}

type RPCHandler func(*RPCContext)

type Route struct {
	prefix      string
	rpc         *RPC
	middlewares []RPCHandler
}

func NewRoute(rpc *RPC, prefix string, middlewares ...RPCHandler) *Route {
	return &Route{
		rpc:         rpc,
		prefix:      prefix,
		middlewares: middlewares,
	}
}

func (r *Route) Use(handlers ...RPCHandler) {
	r.middlewares = append(r.middlewares, handlers...)
}

func (r *Route) Handle(apiPath string, handlers ...RPCHandler) {

	uri := fmt.Sprintf("%s.%s", r.prefix, apiPath)

	logger.Info("Registered API",
		zap.String("path", uri),
	)

	conn := r.rpc.connection
	conn.QueueSubscribe(uri, "system", func(m *nats.Msg) {

		logger.Info("-> " + uri)

		ctx := &RPCContext{}
		ctx.Req.Header = make(map[string]interface{})
		ctx.Req.Data = m.Data
		ctx.Res.ContentType = ContentType_JSON

		for k, v := range m.Header {
			ctx.Req.Header[k] = v
		}

		defer func() {
			var data []byte

			if ctx.Res.Data != nil {
				if ctx.Res.ContentType == ContentType_JSON {
					buf, _ := json.Marshal(ctx.Res.Data)
					data = buf
				}
			}

			// Response
			reply := nats.NewMsg(m.Reply)
			reply.Header = ctx.Res.Header
			reply.Data = data

			//err := m.Respond(data)
			err := m.RespondMsg(reply)
			if err != nil {
				logger.Error(err.Error())
				return
			}
		}()

		// Default middleware
		for _, middleware := range r.middlewares {
			middleware(ctx)

			if ctx.Res.Error != nil {
				logger.Error(ctx.Res.Error.Error())
				return
			}
		}

		// Customized handlers
		for _, h := range handlers {
			h(ctx)

			if ctx.Res.Error != nil {
				logger.Error(ctx.Res.Error.Error())
				return
			}
		}
	})
}
