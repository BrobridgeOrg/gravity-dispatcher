package internal

import (
	"fmt"

	"github.com/BrobridgeOrg/gravity-sdk/config_store"
	"github.com/BrobridgeOrg/gravity-sdk/core"
	"github.com/nats-io/nats.go"
)

type ConfigManager struct {
	client      *core.Client
	configStore *config_store.ConfigStore

	entries map[string]*config_store.ConfigEntry
}

func NewConfigManager(client *core.Client, domain string) *ConfigManager {

	cm := &ConfigManager{
		client:  client,
		entries: make(map[string]*config_store.ConfigEntry),
	}

	cm.configStore = config_store.NewConfigStore(client,
		config_store.WithDomain(domain),
		config_store.WithCatalog("CONFIG"),
		config_store.WithEventHandler(cm.updated),
	)

	err := cm.configStore.Init()
	if err != nil {
		fmt.Println(err)
		return nil
	}

	return cm
}

func (cm *ConfigManager) updated(entry *config_store.ConfigEntry) {

	switch entry.Operation {
	case config_store.ConfigDelete:
		delete(cm.entries, entry.Key)
	case config_store.ConfigUpdate:
		fallthrough
	case config_store.ConfigCreate:
		cm.entries[entry.Key] = entry
	}
}

func (cm *ConfigManager) GetEntry(key string) *config_store.ConfigEntry {

	if entry, ok := cm.entries[key]; ok {
		return entry
	}

	return nil
}

func (cm *ConfigManager) InitializeEntry(key string, initialFn func() []byte) ([]byte, error) {

	entry, err := cm.configStore.Get(key)
	if err == nil {
		return entry.Value(), nil
	}

	if err == nats.ErrKeyNotFound {
		value := initialFn()
		_, err = cm.configStore.Put(key, value)
		if err != nil {
			return nil, err
		}

		return value, nil
	}

	return nil, err
}
