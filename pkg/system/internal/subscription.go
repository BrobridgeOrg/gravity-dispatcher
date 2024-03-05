package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/BrobridgeOrg/gravity-sdk/v2/config_store"
	"github.com/BrobridgeOrg/gravity-sdk/v2/core"
	"github.com/BrobridgeOrg/gravity-sdk/v2/subscription"
	"github.com/nats-io/nats.go"
)

var (
	ErrSubscriptionNotFound      = errors.New("subscription not found")
	ErrSubscriptionExistsAlready = errors.New("subscription exists already")
	ErrInvalidSubscriptionID     = errors.New("invalid subscription ID")
)

type SubscriptionManager struct {
	client      *core.Client
	configStore *config_store.ConfigStore
}

func NewSubscriptionManager(client *core.Client, domain string) *SubscriptionManager {

	sm := &SubscriptionManager{
		client: client,
	}

	sm.configStore = config_store.NewConfigStore(client,
		config_store.WithDomain(domain),
		config_store.WithCatalog("TOKEN"),
	)

	err := sm.configStore.Init()
	if err != nil {
		fmt.Println(err)
		return nil
	}

	return sm
}

func (sm *SubscriptionManager) CreateSubscription(id string, subscriptionSetting *subscription.SubscriptionSetting) (*subscription.SubscriptionSetting, error) {

	// Attempt to get subscription information
	_, err := sm.configStore.Get(id)
	if err != nats.ErrKeyNotFound {
		return nil, ErrSubscriptionExistsAlready
	}

	if err == nats.ErrInvalidKey {
		return nil, ErrInvalidSubscriptionID
	}

	subscriptionSetting.CreatedAt = time.Now()
	subscriptionSetting.UpdatedAt = time.Now()

	data, _ := json.Marshal(subscriptionSetting)

	// Write to KV store
	_, err = sm.configStore.Put(id, data)
	if err != nil {

		switch err {
		case nats.ErrInvalidKey:
			return nil, ErrInvalidSubscriptionID
		}

		return nil, err
	}

	return subscriptionSetting, nil
}

func (sm *SubscriptionManager) DeleteSubscription(id string) error {

	// Check whether specific subscription exist or not
	_, err := sm.GetSubscription(id)
	if err != nil {
		return err
	}

	err = sm.configStore.Delete(id)
	if err != nil {
		return err
	}

	return nil
}

func (sm *SubscriptionManager) UpdateSubscription(id string, subscriptionSetting *subscription.SubscriptionSetting) (*subscription.SubscriptionSetting, error) {

	// Check whether specific subscription exist or not
	_, err := sm.GetSubscription(id)
	if err != nil {
		return nil, err
	}

	subscriptionSetting.UpdatedAt = time.Now()

	data, _ := json.Marshal(subscriptionSetting)

	// Write to KV store
	_, err = sm.configStore.Put(id, data)
	if err != nil {

		switch err {
		case nats.ErrInvalidKey:
			return nil, ErrInvalidSubscriptionID
		}

		return nil, err
	}

	return subscriptionSetting, nil
}

func (sm *SubscriptionManager) GetSubscription(id string) (*subscription.SubscriptionSetting, error) {

	// Attempt to get subscription information
	kv, err := sm.configStore.Get(id)
	if err != nil {
		switch err {
		case nats.ErrInvalidKey:
			fallthrough
		case nats.ErrKeyNotFound:
			return nil, ErrSubscriptionNotFound
		}

		return nil, err
	}

	// Parsing value
	var subscriptionSetting subscription.SubscriptionSetting
	err = json.Unmarshal(kv.Value(), &subscriptionSetting)
	if err != nil {
		return nil, err
	}

	return &subscriptionSetting, nil
}

func (sm *SubscriptionManager) ListSubscriptions() ([]*subscription.SubscriptionSetting, error) {

	// Getting all entries
	keys, _ := sm.configStore.Keys()

	entries := make([]nats.KeyValueEntry, len(keys))
	for i, key := range keys {

		entry, err := sm.configStore.Get(key)
		if err != nil {
			fmt.Printf("Can not get subscription \"%s\" information\n", key)
			continue
		}

		entries[i] = entry
	}

	subscriptions := make([]*subscription.SubscriptionSetting, len(entries))
	for i, entry := range entries {

		var p subscription.SubscriptionSetting
		err := json.Unmarshal(entry.Value(), &p)
		if err != nil {
			fmt.Printf("Subscription \"%s\" Invalid setting format\n", entry.Key())
		}

		subscriptions[i] = &p
	}

	return subscriptions, nil
}
