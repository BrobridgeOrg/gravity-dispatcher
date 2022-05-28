package system

import (
	"context"
	"errors"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/configs"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var logger *zap.Logger
var system *System

type System struct {
	config    *configs.Config
	connector *connector.Connector

	sysConfig  *Config
	coreRPC    *CoreRPC
	productRPC *ProductRPC
	tokenRPC   *TokenRPC
}

func New(lifecycle fx.Lifecycle, config *configs.Config, l *zap.Logger, c *connector.Connector) *System {

	logger = l.Named("System")

	s := &System{
		config:    config,
		connector: c,
	}

	system = s

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

	return s
}

func (system *System) initialize() error {

	logger.Info("Loading system configuration...")

	// Initialize system configuration
	system.sysConfig = NewConfig(system.connector)
	if system.sysConfig == nil {
		return errors.New("Failed to create config client")
	}

	// Initializing RPC hanlers
	system.coreRPC = NewCoreRPC(system)
	err := system.coreRPC.initialize()
	if err != nil {
		return err
	}

	system.productRPC = NewProductRPC(system)
	err = system.productRPC.initialize()
	if err != nil {
		return err
	}

	system.tokenRPC = NewTokenRPC(system)
	err = system.tokenRPC.initialize()
	if err != nil {
		return err
	}

	return nil
}
