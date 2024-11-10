package connector

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/BrobridgeOrg/gravity-sdk/v2/core"
	"github.com/nats-io/nats-server/v2/server"
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
	DefaultConnectionMode      = "default"
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
	viper.SetDefault("gravity.connectionMode", DefaultConnectionMode)

	// Get domain
	domain := viper.GetString("gravity.domain")
	c.domain = domain

	// Connection mode is leaf node
	connectionMode := viper.GetString("gravity.connectionMode")
	if connectionMode == "leaf" {
		err := c.StartLeafNode()
		if err != nil {
		}
	}

	// Initializing client
	client, err := c.CreateClient()
	if err != nil {
		//c.logger.Error(err.Error())
		return err
	}

	c.client = client

	return nil
}

func (c *Connector) StartLeafNode() error {

	host := viper.GetString("gravity.host")
	port := viper.GetInt("gravity.port")

	logger.Info("Starting leaf node...",
		zap.String("targetHost", host),
		zap.Int("targetPort", port),
	)

	serverUrl := fmt.Sprintf("nats://%s:%d", host, port)
	u, err := url.Parse(serverUrl)
	if err != nil {
		return err
	}

	opts := &server.Options{
		JetStream: true,
		LeafNode: server.LeafNodeOpts{
			Remotes: []*server.RemoteLeafOpts{
				{
					URLs: []*url.URL{u},
				},
			},
		},
	}
	s, err := server.NewServer(opts)
	if err != nil {
		return err
	}

	go s.Start()

	timeout := 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("leaf node start timed out after %v", timeout)
		}
	case <-time.After(500 * time.Millisecond):
		if !s.ReadyForConnections(timeout) {
			return fmt.Errorf("leaf node failed to become ready within timeout period")
		}

		logger.Info("Leaf node is ready")
	}

	return nil
}

func (c *Connector) CreateClient() (*core.Client, error) {

	// Read configs
	domain := viper.GetString("gravity.domain")
	//	accessKey := viper.GetString("gravity.accessKey")

	host := viper.GetString("gravity.host")
	port := viper.GetInt("gravity.port")
	connectionMode := viper.GetString("gravity.connectionMode")
	if connectionMode == "leaf" {
		host = "localhost"
		port = 4222
	}

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
