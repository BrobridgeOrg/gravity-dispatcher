package dispatcher

import (
	"encoding/json"
	"fmt"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/configs"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/config_store"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/message"
	"go.uber.org/zap"
)

var logger *zap.Logger

type Dispatcher struct {
	config                *configs.Config
	connector             *connector.Connector
	collectionConfigStore *config_store.ConfigStore
	collectionManager     *CollectionManager
	watcher               *EventWatcher
	processor             *Processor
}

func New(config *configs.Config, l *zap.Logger, c *connector.Connector) *Dispatcher {

	logger = l

	d := &Dispatcher{
		config:    config,
		connector: c,
	}

	d.collectionManager = NewCollectionManager(d)
	d.processor = NewProcessor(
		WithOutputHandler(func(msg *message.Message) {
			d.dispatch(msg)
		}),
	)
	d.collectionConfigStore = config_store.NewConfigStore(c,
		config_store.WithDomain(c.GetDomain()),
		config_store.WithCatalog("COLLECTION"),
		config_store.WithEventHandler(d.collectionSettingsUpdated),
	)

	err := d.initialize()
	if err != nil {
		logger.Error(err.Error())
		return nil
	}

	return d
}

func (d *Dispatcher) collectionSettingsUpdated(op config_store.ConfigOp, collectionName string, data []byte) {

	logger.Info("Syncing collection settings",
		zap.String("collection", collectionName),
	)

	var setting CollectionSetting
	err := json.Unmarshal(data, &setting)
	if err != nil {
		logger.Error(err.Error())
		return
	}

	// Delete collection
	if op == config_store.ConfigDelete {
		d.collectionManager.DeleteCollection(collectionName)
		return
	}

	// Create or update collection
	err = d.collectionManager.ApplySettings(collectionName, &setting)
	if err != nil {
		logger.Error("Failed to load collection settings",
			zap.String("collection", collectionName),
		)
		logger.Error(err.Error())
		return
	}
}

func (d *Dispatcher) initialize() error {

	err := d.collectionConfigStore.Init()
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
func (d *Dispatcher) dispatch(msg *message.Message) {

	subject := fmt.Sprintf("GRAVITY.%s.COLLECTION.%s.%d.EVENT.%s",
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

		// Publish to collection stream
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
