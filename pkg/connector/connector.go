package connector

import (
	"fmt"
	"time"

	"github.com/BrobridgeOrg/gravity-sdk/core"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const (
	DefaultHost                = "0.0.0.0"
	DefaultPort                = 32803
	DefaultPingInterval        = 10
	DefaultMaxPingsOutstanding = 3
	DefaultMaxReconnects       = -1
	DefaultAccessKey           = ""
	DefaultDomain              = "gravity"
)

type Connector struct {
	client *core.Client
	logger *zap.Logger
	domain string
}

func New(logger *zap.Logger) *Connector {

	c := &Connector{
		client: core.NewClient(),
		logger: logger,
	}

	c.initialize()

	return c
}

func (c *Connector) initialize() {

	err := c.connect()
	if err != nil {
		c.logger.Error(err.Error())
	}
}

func (c *Connector) connect() error {

	// default domain and access key
	viper.SetDefault("gravity.domain", DefaultDomain)
	viper.SetDefault("gravity.accessKey", DefaultAccessKey)

	// default settings
	viper.SetDefault("gravity.host", DefaultHost)
	viper.SetDefault("gravity.port", DefaultPort)
	viper.SetDefault("gravity.pingInterval", DefaultPingInterval)
	viper.SetDefault("gravity.maxPingsOutstanding", DefaultMaxPingsOutstanding)
	viper.SetDefault("gravity.maxReconnects", DefaultMaxReconnects)

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

	c.logger.Info("Connecting to Gravity Network...",
		zap.String("domain", domain),
		zap.String("address", address),
		zap.Duration("pingInterval", options.PingInterval),
		zap.Int("maxPingsOutstanding", options.MaxPingsOutstanding),
		zap.Int("maxReconnects", options.MaxReconnects),
	)

	c.domain = domain

	// Initializing keyring
	//keyInfo := synchronizer.keyring.Put("gravity", accessKey)
	//keyInfo.Permission().AddPermissions([]string{"SYSTEM"})

	// Connect
	return c.client.Connect(address, options)
}

func (c *Connector) GetClient() *core.Client {
	return c.client
}

func (c *Connector) GetDomain() string {
	return c.domain
}
