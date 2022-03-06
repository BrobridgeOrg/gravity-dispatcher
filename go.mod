module github.com/BrobridgeOrg/gravity-dispatcher

go 1.15

require (
	github.com/BrobridgeOrg/gravity-api v0.2.25
	github.com/BrobridgeOrg/gravity-sdk v0.0.50
	github.com/BrobridgeOrg/schemer v0.0.10
	github.com/BrobridgeOrg/sequential-data-flow v0.0.1
	github.com/cfsghost/buffered-input v0.0.1
	github.com/cfsghost/gosharding v0.0.3
	github.com/cfsghost/taskflow v0.0.2
	github.com/d5/tengo v1.24.8
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.1.2
	github.com/json-iterator/go v1.1.12
	github.com/lithammer/go-jump-consistent-hash v1.0.2
	github.com/nats-io/nats.go v1.13.1-0.20220121202836-972a071d373d
	github.com/prometheus/common v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.3.0
	github.com/spf13/viper v1.10.1
	go.uber.org/fx v1.16.0
	go.uber.org/zap v1.17.0
)

replace github.com/BrobridgeOrg/gravity-sdk => ../gravity-sdk
