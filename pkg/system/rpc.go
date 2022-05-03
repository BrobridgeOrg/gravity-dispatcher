package system

import (
	"fmt"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	"github.com/nats-io/nats.go"
)

type RPC struct {
	connection *nats.Conn
	routes     map[string]*Route
}

func NewRPC(connector *connector.Connector) RPC {
	return RPC{
		connection: connector.GetClient().GetConnection(),
		routes:     make(map[string]*Route),
	}
}

func (rpc *RPC) createRoute(name string, prefix string) (*Route, error) {

	route := NewRoute(rpc, prefix)

	err := rpc.registerRoute(name, route)
	if err != nil {
		return nil, err
	}

	return route, nil
}

func (rpc *RPC) registerRoute(name string, route *Route) error {

	if _, ok := rpc.routes[name]; ok {
		return fmt.Errorf("route \"%s\" exists already", name)
	}

	rpc.routes[name] = route

	return nil
}
