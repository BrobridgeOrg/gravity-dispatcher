package dispatcher

import (
	"encoding/json"
	"fmt"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/configs"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	"github.com/BrobridgeOrg/gravity-sdk/config_store"
	product_sdk "github.com/BrobridgeOrg/gravity-sdk/product"
	"go.uber.org/zap"
)

var logger *zap.Logger

type Dispatcher struct {
	config             *configs.Config
	connector          *connector.Connector
	productConfigStore *config_store.ConfigStore
	productManager     *ProductManager
	watcher            *EventWatcher
	processor          *Processor
}

func New(config *configs.Config, l *zap.Logger, c *connector.Connector) *Dispatcher {

	logger = l

	d := &Dispatcher{
		config:    config,
		connector: c,
	}

	d.productManager = NewProductManager(d)
	d.processor = NewProcessor(
		WithOutputHandler(func(msg *Message) {
			d.dispatch(msg)
		}),
	)
	d.productConfigStore = config_store.NewConfigStore(c.GetClient(),
		config_store.WithDomain(c.GetDomain()),
		config_store.WithCatalog("PRODUCT"),
		config_store.WithEventHandler(d.productSettingsUpdated),
	)

	err := d.initialize()
	if err != nil {
		logger.Error(err.Error())
		return nil
	}

	return d
}

func (d *Dispatcher) productSettingsUpdated(op config_store.ConfigOp, productName string, data []byte) {

	logger.Info("Syncing data product settings",
		zap.String("name", productName),
	)

	var setting product_sdk.ProductSetting
	err := json.Unmarshal(data, &setting)
	if err != nil {
		logger.Error(err.Error())
		return
	}

	// Delete product
	if op == config_store.ConfigDelete {
		d.productManager.DeleteProduct(productName)
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
func (d *Dispatcher) dispatch(msg *Message) {

	subject := fmt.Sprintf("GRAVITY.%s.DP.%s.%d.EVENT.%s",
		d.connector.GetDomain(),
		msg.Record.Table,
		msg.Partition,
		msg.Record.EventName,
	)

	// Preparing JetStream
	nc := d.connector.GetClient().GetConnection()
	js, err := nc.JetStream()
	if err != nil {
		logger.Error(err.Error())
		return
	}

	go func() {

		// Publish to product stream
		future, err := js.PublishAsync(subject, msg.RawRecord)
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
