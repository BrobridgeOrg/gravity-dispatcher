package config_store

import (
	"fmt"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	"github.com/nats-io/nats.go"
)

type ConfigOp int32

const (
	ConfigCreate ConfigOp = iota
	ConfigUpdate
	ConfigDelete
)

type ConfigStore struct {
	connector    *connector.Connector
	domain       string
	catalog      string
	watcher      nats.KeyWatcher
	eventHandler func(ConfigOp, string, []byte)
}

func NewConfigStore(connector *connector.Connector, opts ...func(*ConfigStore)) *ConfigStore {

	cs := &ConfigStore{
		connector: connector,
	}

	for _, opt := range opts {
		opt(cs)
	}

	return cs
}

func WithDomain(domain string) func(*ConfigStore) {
	return func(cs *ConfigStore) {
		cs.domain = domain
	}
}

func WithCatalog(catalog string) func(*ConfigStore) {
	return func(cs *ConfigStore) {
		cs.catalog = catalog
	}
}

func WithEventHandler(fn func(ConfigOp, string, []byte)) func(*ConfigStore) {
	return func(cs *ConfigStore) {
		cs.eventHandler = fn
	}
}

func (cs *ConfigStore) Init() error {

	// Preparing JetStream
	nc := cs.connector.GetClient().GetConnection()
	js, err := nc.JetStream()
	if err != nil {
		return err
	}

	bucket := fmt.Sprintf("GRAVITY_%s_%s", cs.domain, cs.catalog)

	// Attempt to create KV store
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:      bucket,
		Description: "Gravity config store",
	})
	if err != nil {
		return err
	}

	// Load configuration from KV store
	kv, err = js.KeyValue(bucket)
	if err != nil {
		return err
	}

	// Getting all entries
	keys, _ := kv.Keys()
	if len(keys) > 0 {

		entries := make([]nats.KeyValueEntry, len(keys))
		for i, key := range keys {

			entry, err := kv.Get(key)
			if err != nil {
				return err
			}

			entries[i] = entry
		}

		for _, entry := range entries {
			cs.eventHandler(ConfigCreate, entry.Key(), entry.Value())
		}
	}

	// Watching event of KV Store for real-time updating
	watcher, err := kv.WatchAll()
	if err != nil {
		return err
	}

	cs.watcher = watcher

	go func() {

		for entry := range watcher.Updates() {

			if entry == nil {
				continue
			}

			var op ConfigOp
			switch entry.Operation() {
			case nats.KeyValuePut:
				op = ConfigUpdate
			case nats.KeyValueDelete:
				op = ConfigDelete
			case nats.KeyValuePurge:
				op = ConfigDelete
			}

			cs.eventHandler(op, entry.Key(), entry.Value())
		}
	}()

	return nil
}
