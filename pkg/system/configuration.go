package system

import (
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	internal "github.com/BrobridgeOrg/gravity-dispatcher/pkg/system/internal"
)

type Config struct {
	configManager *internal.ConfigManager
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

	_, err := cfg.configManager.InitializeEntry("secret", func() []byte {

		logger.Info("Initializing secret configurations...")

		// Genereate a new key for initializing
		key, _ := internal.GenerateRandomString(64)
		secret := internal.ConfigEntrySecret{
			Key: key,
		}

		data, _ := json.Marshal(secret)

		return data
	})
	if err != nil {
		return err
	}

	_, err = cfg.configManager.InitializeEntry("auth", func() []byte {

		logger.Info("Initializing auth configurations...")

		// Disabled auth by default
		authConfig := internal.ConfigEntryAuth{
			Enabled: false,
		}

		data, _ := json.Marshal(authConfig)

		return data
	})
	if err != nil {
		return err
	}

	return nil
}

func (cfg *Config) GetEntry(key string) internal.IConfigEntry {
	return cfg.configManager.GetEntry(key)
}
