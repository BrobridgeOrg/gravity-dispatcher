package connector

import (
	"context"
	"fmt"
	"time"

	"github.com/BrobridgeOrg/gravity-sdk/core"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var logger *zap.Logger

const (
	DefaultHost                = "0.0.0.0"
	DefaultPort                = 32803
	DefaultPingInterval        = 10
	DefaultMaxPingsOutstanding = 3
	DefaultMaxReconnects       = -1
	DefaultAccessKey           = ""
	DefaultDomain              = "default"
)

type Connector struct {
	client *core.Client
	logger *zap.Logger
	domain string
}

func New(lifecycle fx.Lifecycle, l *zap.Logger) *Connector {

	logger = l.Named("Connector")

	c := &Connector{}

	//c.initialize()

	lifecycle.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				return c.initialize()
			},
			OnStop: func(ctx context.Context) error {
				c.client.Disconnect()
				return nil
			},
		},
	)

	return c
}

func (c *Connector) initialize() error {

	// default domain and access key
	viper.SetDefault("gravity.domain", DefaultDomain)
	viper.SetDefault("gravity.accessKey", DefaultAccessKey)

	// default settings
	viper.SetDefault("gravity.host", DefaultHost)
	viper.SetDefault("gravity.port", DefaultPort)
	viper.SetDefault("gravity.pingInterval", DefaultPingInterval)
	viper.SetDefault("gravity.maxPingsOutstanding", DefaultMaxPingsOutstanding)
	viper.SetDefault("gravity.maxReconnects", DefaultMaxReconnects)

	// Get domain
	domain := viper.GetString("gravity.domain")
	c.domain = domain

	// Initializing client
	client, err := c.CreateClient()
	if err != nil {
		//c.logger.Error(err.Error())
		return err
	}

	c.client = client

	return nil
}

func (c *Connector) CreateClient() (*core.Client, error) {

	// Read configs
	domain := viper.GetString("gravity.domain")
	//	accessKey := viper.GetString("gravity.accessKey")
	host := viper.GetString("gravity.host")
	port := viper.GetInt("gravity.port")
	pingInterval := viper.GetInt64("gravity.pingInterval")
	maxPingsOutstanding := viper.GetInt("gravity.maxPingsOutstanding")
	maxReconnects := viper.GetInt("gravity.maxReconnects")

	// Preparing options
	options := core.NewOptions()
	options.PingInterval = time.Duration(pingInterval) * time.Second
	options.MaxPingsOutstanding = maxPingsOutstanding
	options.MaxReconnects = maxReconnects

	address := fmt.Sprintf("%s:%d", host, port)

	logger.Info("Connecting to Gravity Network...",
		zap.String("domain", domain),
		zap.String("address", address),
		zap.Duration("pingInterval", options.PingInterval),
		zap.Int("maxPingsOutstanding", options.MaxPingsOutstanding),
		zap.Int("maxReconnects", options.MaxReconnects),
	)

	client := core.NewClient()
	err := client.Connect(address, options)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (c *Connector) GetClient() *core.Client {
	return c.client
}

func (c *Connector) GetDomain() string {
	return c.domain
}
