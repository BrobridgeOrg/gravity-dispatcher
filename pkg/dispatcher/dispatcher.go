package dispatcher

import (
	"context"
	"time"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/configs"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	"github.com/BrobridgeOrg/gravity-sdk/config_store"
	"github.com/BrobridgeOrg/gravity-sdk/core"
	product_sdk "github.com/BrobridgeOrg/gravity-sdk/product"
	buffered_input "github.com/cfsghost/buffered-input"
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
	processor          *Processor
	dispatcherBuffer   *buffered_input.BufferedInput
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

	// Preparing publisher with individual connection
	logger.Info("Initializing publisher with individual connection...")
	err := d.initializePublisher()
	if err != nil {
		return err
	}

	// Initializing buffered input
	opts := buffered_input.NewOptions()
	opts.ChunkSize = 1000
	opts.ChunkCount = 1000
	opts.Timeout = 100 * time.Millisecond
	opts.Handler = d.dispatcherBufferHandler
	d.dispatcherBuffer = buffered_input.NewBufferedInput(opts)

	d.processor = NewProcessor(
		WithDomain(d.connector.GetDomain()),
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

func (d *Dispatcher) handleProductEvents(product *Product, eventName string, m *Message) {
	d.processor.Push(m)
}

func (d *Dispatcher) dispatcherBufferHandler(chunk []interface{}) {

	var prev *Message
	for _, v := range chunk {
		select {
		case <-v.(*Message).Future.Ok():
			prev = v.(*Message)
		case e := <-v.(*Message).Future.Err():
			logger.Error("Failed to publish message",
				zap.Error(e),
			)
			prev.Msg.Ack()

			if prev != nil {
				// Ack the last message
				prev.Msg.Ack()
				prev.Release()
			}

			return
		}
	}

	prev.Msg.Ack()
	prev.Release()
}

func (d *Dispatcher) dispatch(msg *Message) {

	// Publish to product stream
	future, err := d.publisherJSCtx.PublishMsgAsync(msg.OutputMsg, nats.MsgId(msg.ID))
	if err != nil {
		logger.Error(err.Error())
		return
	}

	msg.Future = future

	d.dispatcherBuffer.Push(msg)
}
