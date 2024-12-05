package dispatcher

import (
	"context"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/configs"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/system"
	"github.com/BrobridgeOrg/gravity-sdk/v2/config_store"
	"github.com/BrobridgeOrg/gravity-sdk/v2/core"
	product_sdk "github.com/BrobridgeOrg/gravity-sdk/v2/product"
	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nats.go"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var logger *zap.Logger

type Dispatcher struct {
	publisher          *core.Client
	publisherJSCtx     nats.JetStreamContext
	config             *configs.Config
	connector          *connector.Connector
	productConfigStore *config_store.ConfigStore
	productManager     *ProductManager
}

func New(lifecycle fx.Lifecycle, config *configs.Config, l *zap.Logger, c *connector.Connector, s *system.System) *Dispatcher {

	logger = l.Named("Dispatcher")

	d := &Dispatcher{
		config:    config,
		connector: c,
	}

	lifecycle.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				return d.initialize()
			},
			OnStop: func(ctx context.Context) error {
				return nil
			},
		},
	)

	return d
}

func (d *Dispatcher) productSettingsUpdated(entry *config_store.ConfigEntry) {

	logger.Info("Syncing data product settings",
		zap.String("name", entry.Key),
	)

	// Delete product
	if entry.Operation == config_store.ConfigDelete {
		logger.Info("Delete product",
			zap.String("product", entry.Key),
		)
		d.productManager.DeleteProduct(entry.Key)
		return
	}

	// Parsing setting
	var setting product_sdk.ProductSetting
	err := json.Unmarshal(entry.Value, &setting)
	if err != nil {
		logger.Error("Failed to sync:",
			zap.Error(err),
			zap.String("op", entry.Operation.String()),
			zap.String("raw", string(entry.Value)),
		)

		return
	}

	// Create or update data product
	err = d.productManager.ApplySettings(entry.Key, &setting)
	if err != nil {
		logger.Error("Failed to load data product settings",
			zap.String("product", entry.Key),
			zap.Error(err),
		)
		return
	}
}

func (d *Dispatcher) initialize() error {

	// Preparing publisher with individual connection
	logger.Info("Initializing publisher with individual connection...")
	err := d.initializePublisher()
	if err != nil {
		return err
	}

	d.productConfigStore = config_store.NewConfigStore(d.connector.GetClient(),
		config_store.WithDomain(d.connector.GetDomain()),
		config_store.WithCatalog("PRODUCT"),
		config_store.WithEventHandler(d.productSettingsUpdated),
	)
	d.productManager = NewProductManager(d)

	logger.Info("Initializing config store...")

	err = d.productConfigStore.Init()
	if err != nil {
		return err
	}

	return nil
}

func (d *Dispatcher) initializePublisher() error {

	client, err := d.connector.CreateClient()
	if err != nil {
		return err
	}

	d.publisher = client

	// Preparing JetStream
	js, err := d.connector.GetClient().GetJetStream()
	if err != nil {
		return err
	}

	d.publisherJSCtx = js

	return nil
}

/*
func (d *Dispatcher) registerEvents() {

	// Default events
	for _, e := range d.config.Events {
		logger.Info(fmt.Sprintf("Regiserted event: %s", e))
		d.watcher.RegisterEvent(e)
	}
}
*/
