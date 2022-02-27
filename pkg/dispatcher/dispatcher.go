package dispatcher

import (
	"context"
	"fmt"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/configs"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	"github.com/BrobridgeOrg/gravity-sdk/config_store"
	product_sdk "github.com/BrobridgeOrg/gravity-sdk/product"
	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nats.go"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var logger *zap.Logger

type Dispatcher struct {
	config             *configs.Config
	connector          *connector.Connector
	productConfigStore *config_store.ConfigStore
	productManager     *ProductManager
	processor          *Processor
}

func New(lifecycle fx.Lifecycle, config *configs.Config, l *zap.Logger, c *connector.Connector) *Dispatcher {

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

func (d *Dispatcher) productSettingsUpdated(op config_store.ConfigOp, productName string, data []byte) {

	logger.Info("Syncing data product settings",
		zap.String("name", productName),
	)

	// Delete product
	if op == config_store.ConfigDelete {
		logger.Info("Delete product",
			zap.String("product", productName),
		)
		d.productManager.DeleteProduct(productName)
		return
	}

	// Parsing setting
	var setting product_sdk.ProductSetting
	err := json.Unmarshal(data, &setting)
	if err != nil {
		logger.Error("Failed to sync:",
			zap.Error(err),
			zap.String("op", op.String()),
			zap.String("raw", string(data)),
		)

		return
	}

	// Create or update data product
	err = d.productManager.ApplySettings(productName, &setting)
	if err != nil {
		logger.Error("Failed to load data product settings",
			zap.String("product", productName),
		)
		logger.Error(err.Error())
		return
	}
}

func (d *Dispatcher) initialize() error {

	d.processor = NewProcessor(
		WithOutputHandler(func(msg *Message) {
			d.dispatch(msg)
		}),
	)
	d.productConfigStore = config_store.NewConfigStore(d.connector.GetClient(),
		config_store.WithDomain(d.connector.GetDomain()),
		config_store.WithCatalog("PRODUCT"),
		config_store.WithEventHandler(d.productSettingsUpdated),
	)
	d.productManager = NewProductManager(d)
	d.productManager.Subscribe(d.handleProductEvents)

	logger.Info("Initializing config store...")

	err := d.productConfigStore.Init()
	if err != nil {
		return err
	}

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

func (d *Dispatcher) handleProductEvents(product *Product, eventName string, m *Message) {
	d.processor.Push(m)
}

func (d *Dispatcher) dispatch(msg *Message) {

	subject := fmt.Sprintf("$GVT.%s.DP.%s.%d.EVENT.%s",
		d.connector.GetDomain(),
		msg.Record.Table,
		msg.Partition,
		msg.Record.EventName,
	)

	// Preparing JetStream
	js, err := d.connector.GetClient().GetJetStream()
	if err != nil {
		logger.Error(err.Error())
		return
	}

	go func() {

		m := &nats.Msg{
			Subject: subject,
			Data:    msg.RawRecord,
		}

		m.Header = msg.Msg.Header

		// Publish to product stream
		future, err := js.PublishMsgAsync(m)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		<-future.Ok()

		// Acknowledge
		msg.Msg.Ack()

		msg.Release()
	}()
}
