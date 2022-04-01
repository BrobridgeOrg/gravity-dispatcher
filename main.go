package main

import (
	"os"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/configs"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/logger"
	"github.com/spf13/cobra"

	"go.uber.org/fx"
)

var config *configs.Config
var events []string

var rootCmd = &cobra.Command{
	Use:   "gravity-dispatcher",
	Short: "Gravity Component to dispatch events",
	Long: `gravity-dispatcheris a component to dispatch incoming events.
This application can be used to recieve and dispatch events from adapter or external clients`,
	RunE: func(cmd *cobra.Command, args []string) error {

		if err := run(); err != nil {
			return err
		}
		return nil
	},
}

func init() {
	config = configs.GetConfig()

	rootCmd.Flags().StringSliceVar(&events, "events", []string{}, "Specify events for watching")
}

func main() {

	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func run() error {

	config.SetConfigs(map[string]interface{}{})
	config.AddEvents(events)

	fx.New(
		fx.Supply(config),
		fx.Provide(
			logger.GetLogger,
			connector.New,
		),
		fx.Invoke(dispatcher.New),
		fx.NopLogger,
	).Run()

	return nil
}
