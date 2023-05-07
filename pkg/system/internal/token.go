package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/BrobridgeOrg/gravity-sdk/v2/config_store"
	"github.com/BrobridgeOrg/gravity-sdk/v2/core"
	"github.com/BrobridgeOrg/gravity-sdk/v2/token"
	"github.com/nats-io/nats.go"
)

var (
	ErrTokenNotFound      = errors.New("token not found")
	ErrTokenExistsAlready = errors.New("token exists already")
	ErrInvalidTokenName   = errors.New("invalid token name")
)

type TokenManager struct {
	client      *core.Client
	configStore *config_store.ConfigStore
}

func NewTokenManager(client *core.Client, domain string) *TokenManager {

	tm := &TokenManager{
		client: client,
	}

	tm.configStore = config_store.NewConfigStore(client,
		config_store.WithDomain(domain),
		config_store.WithCatalog("TOKEN"),
	)

	err := tm.configStore.Init()
	if err != nil {
		fmt.Println(err)
		return nil
	}

	return tm
}

func (tm *TokenManager) CreateToken(t string, tokenSetting *token.TokenSetting) (*token.TokenSetting, error) {

	// Attempt to get token information
	_, err := tm.configStore.Get(t)
	if err != nats.ErrKeyNotFound {
		return nil, ErrTokenExistsAlready
	}

	if err == nats.ErrInvalidKey {
		return nil, ErrInvalidTokenName
	}

	tokenSetting.CreatedAt = time.Now()
	tokenSetting.UpdatedAt = time.Now()

	data, _ := json.Marshal(tokenSetting)

	// Write to KV store
	_, err = tm.configStore.Put(t, data)
	if err != nil {

		switch err {
		case nats.ErrInvalidKey:
			return nil, ErrInvalidTokenName
		}

		return nil, err
	}

	return tokenSetting, nil
}

func (tm *TokenManager) DeleteToken(t string) error {

	// Check whether specific token exist or not
	_, err := tm.GetToken(t)
	if err != nil {
		return err
	}

	err = tm.configStore.Delete(t)
	if err != nil {
		return err
	}

	return nil
}

func (tm *TokenManager) UpdateToken(t string, tokenSetting *token.TokenSetting) (*token.TokenSetting, error) {

	// Check whether specific token exist or not
	_, err := tm.GetToken(t)
	if err != nil {
		return nil, err
	}

	tokenSetting.UpdatedAt = time.Now()

	data, _ := json.Marshal(tokenSetting)

	// Write to KV store
	_, err = tm.configStore.Put(t, data)
	if err != nil {

		switch err {
		case nats.ErrInvalidKey:
			return nil, ErrInvalidTokenName
		}

		return nil, err
	}

	return tokenSetting, nil
}

func (tm *TokenManager) GetToken(t string) (*token.TokenSetting, error) {

	// Attempt to get token information
	kv, err := tm.configStore.Get(t)
	if err != nil {
		switch err {
		case nats.ErrInvalidKey:
			fallthrough
		case nats.ErrKeyNotFound:
			return nil, ErrTokenNotFound
		}

		return nil, err
	}

	// Parsing value
	var tokenSetting token.TokenSetting
	err = json.Unmarshal(kv.Value(), &tokenSetting)
	if err != nil {
		return nil, err
	}

	return &tokenSetting, nil
}

func (tm *TokenManager) ListTokens() ([]*token.TokenSetting, error) {

	// Getting all entries
	keys, _ := tm.configStore.Keys()

	entries := make([]nats.KeyValueEntry, len(keys))
	for i, key := range keys {

		entry, err := tm.configStore.Get(key)
		if err != nil {
			fmt.Printf("Can not get token \"%s\" information\n", key)
			continue
		}

		entries[i] = entry
	}

	tokens := make([]*token.TokenSetting, len(entries))
	for i, entry := range entries {

		var p token.TokenSetting
		err := json.Unmarshal(entry.Value(), &p)
		if err != nil {
			fmt.Printf("Token \"%s\" Invalid setting format\n", entry.Key())
		}

		tokens[i] = &p
	}

	return tokens, nil
}
