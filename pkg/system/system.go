package system

import (
	"context"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/configs"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var logger *zap.Logger

type System struct {
	config    *configs.Config
	connector *connector.Connector

	productRPC *ProductRPC
}

func New(lifecycle fx.Lifecycle, config *configs.Config, l *zap.Logger, c *connector.Connector) *System {

	logger = l.Named("System")

	system := &System{
		config:    config,
		connector: c,
	}

	lifecycle.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				return system.initialize()
			},
			OnStop: func(ctx context.Context) error {
				return nil
			},
		},
	)

	return system
}

func (system *System) initialize() error {

	logger.Info("Initializing core...")

	system.productRPC = NewProductRPC(system.connector)
	err := system.productRPC.initialize()
	if err != nil {
		return err
	}

	return nil
}
