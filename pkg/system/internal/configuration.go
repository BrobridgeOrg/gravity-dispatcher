package internal

import (
	"encoding/json"
	"fmt"

	"github.com/BrobridgeOrg/gravity-sdk/v2/config_store"
	"github.com/BrobridgeOrg/gravity-sdk/v2/core"
	"github.com/nats-io/nats.go"
)

type ConfigManager struct {
	client      *core.Client
	configStore *config_store.ConfigStore

	entries map[string]*ConfigEntry
}

func NewConfigManager(client *core.Client, domain string) *ConfigManager {

	cm := &ConfigManager{
		client:  client,
		entries: make(map[string]*ConfigEntry),
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
		err := cm.updatedEntry(entry)
		if err != nil {
			fmt.Printf("Failed to update config entry \"%s\" : %v\n", entry.Key, err)
		}
	}
}

func (cm *ConfigManager) updatedEntry(entry *config_store.ConfigEntry) error {

	switch entry.Key {
	case "secret":

		// Parsing
		var secret ConfigEntrySecret
		err := json.Unmarshal(entry.Value, &secret)
		if err != nil {
			return err
		}

		cm.entries[entry.Key] = &ConfigEntry{
			secret: &secret,
		}
	case "auth":

		// Parsing
		var auth ConfigEntryAuth
		err := json.Unmarshal(entry.Value, &auth)
		if err != nil {
			return err
		}

		cm.entries[entry.Key] = &ConfigEntry{
			auth: &auth,
		}
	}

	return nil
}

func (cm *ConfigManager) GetEntry(key string) *ConfigEntry {

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

		// TODO: It should check revision of entry
		_, err = cm.configStore.Put(key, value)
		if err != nil {
			return nil, err
		}

		return value, nil
	}

	return nil, err
}

func (cm *ConfigManager) SetEntry(key string, value []byte) error {

	_, err := cm.configStore.Put(key, value)
	if err != nil {
		return err
	}

	return nil
}

// Configuration entry interface
type ConfigEntrySecret struct {
	Key string `json:"key"`
}

type ConfigEntryAuth struct {
	Enabled bool `json:"enabled"`
}

type IConfigEntry interface {
	Secret() *ConfigEntrySecret
	Auth() *ConfigEntryAuth
}

type ConfigEntry struct {
	secret *ConfigEntrySecret
	auth   *ConfigEntryAuth
}

func (ce *ConfigEntry) Secret() *ConfigEntrySecret {
	return ce.secret
}

func (ce *ConfigEntry) Auth() *ConfigEntryAuth {
	return ce.auth
}
