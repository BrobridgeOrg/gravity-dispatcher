package system

import (
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	internal "github.com/BrobridgeOrg/gravity-dispatcher/pkg/system/internal"
)

type Secret struct {
	Key string `json:"key"`
}

type Config struct {
	configManager *internal.ConfigManager

	secret *Secret
}

func NewConfig(c *connector.Connector) *Config {

	cfg := &Config{}

	// Initialize config manager
	cfg.configManager = internal.NewConfigManager(
		c.GetClient(),
		c.GetDomain(),
	)

	if cfg.configManager == nil {
		return nil
	}

	err := cfg.initialize()
	if err != nil {
		logger.Error(err.Error())
		return nil
	}

	return cfg
}

func (cfg *Config) initialize() error {

	value, err := cfg.configManager.InitializeEntry("secret", func() []byte {

		logger.Info("Initializing secret configurations...")

		// Genereate a new key for initializing
		key, _ := internal.GenerateRandomString(64)
		secret := Secret{
			Key: key,
		}

		data, _ := json.Marshal(secret)

		return data
	})
	if err != nil {
		return err
	}

	// Parsing
	var secret Secret
	err = json.Unmarshal(value, &secret)
	if err != nil {
		return err
	}

	cfg.secret = &secret

	return nil
}

func (cfg *Config) GetSecret(key string) (*Secret, error) {

	// Getting original config entry
	entry := cfg.configManager.GetEntry(key)
	if entry == nil {
		return nil, nil
	}

	// Parsing
	var secret Secret
	err := json.Unmarshal(entry.Value, &secret)
	if err != nil {
		return nil, err
	}

	return &secret, nil
}
