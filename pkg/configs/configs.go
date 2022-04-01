package configs

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Events []string
}

func GetConfig() *Config {

	// From the environment
	viper.SetEnvPrefix("GRAVITY_DISPATCHER")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// From config file
	viper.SetConfigName("config")
	viper.AddConfigPath("./")
	viper.AddConfigPath("./configs")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("No configuration file was loaded")
	}

	runtime.GOMAXPROCS(8)

	config := &Config{
		Events: make([]string, 0),
	}

	// Specify events from environment variable for watching
	events := viper.GetStringSlice("EVENTS")
	for _, e := range events {
		config.Events = append(config.Events, e)
	}

	return config
}

func (config *Config) FindEvents(event string) int {

	for i, e := range config.Events {
		if event == e {
			return i
		}
	}

	return -1
}

func (config *Config) AddEvents(events []string) {

	for _, event := range events {
		if config.FindEvents(event) == -1 {
			config.Events = append(config.Events, event)
		}
	}
}

func (config *Config) SetConfigs(configs map[string]interface{}) {

	for k, v := range configs {
		if !viper.IsSet(k) {
			viper.Set(k, v)
		}
	}
}
