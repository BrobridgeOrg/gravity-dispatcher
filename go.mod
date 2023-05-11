module github.com/BrobridgeOrg/gravity-dispatcher

go 1.15

require (
	github.com/BrobridgeOrg/compton v0.0.0-20220617174904-7083c8a5102d
	github.com/BrobridgeOrg/gravity-sdk/v2 v2.0.3
	github.com/BrobridgeOrg/schemer v0.0.10
	github.com/BrobridgeOrg/sequential-data-flow v0.0.2
	github.com/cfsghost/buffered-input v0.0.3
	github.com/d5/tengo v1.24.8
	github.com/dop251/goja v0.0.0-20220214123719-b09a6bfa842f // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible
	github.com/google/uuid v1.3.0
	github.com/json-iterator/go v1.1.12
	github.com/lithammer/go-jump-consistent-hash v1.0.2
	github.com/magiconair/properties v1.8.6 // indirect
	github.com/nats-io/nats.go v1.25.0
	github.com/spf13/afero v1.8.1 // indirect
	github.com/spf13/cobra v1.3.0
	github.com/spf13/viper v1.10.1
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/fx v1.17.0
	go.uber.org/multierr v1.8.0 // indirect
	go.uber.org/zap v1.21.0
	gopkg.in/ini.v1 v1.66.4 // indirect
)

//replace github.com/BrobridgeOrg/gravity-sdk/v2 => ../gravity-sdk

// replace github.com/BrobridgeOrg/compton => ../../compton
