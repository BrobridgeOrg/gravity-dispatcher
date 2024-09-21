module github.com/BrobridgeOrg/gravity-dispatcher

go 1.21

require (
	github.com/BrobridgeOrg/gravity-sdk/v2 v2.0.7
	github.com/BrobridgeOrg/schemer v0.0.13
	github.com/BrobridgeOrg/sequential-data-flow v0.0.2
	github.com/cfsghost/buffered-input v0.0.3
	github.com/d5/tengo v1.24.8
	github.com/golang-jwt/jwt v3.2.2+incompatible
	github.com/google/uuid v1.3.0
	github.com/json-iterator/go v1.1.12
	github.com/lithammer/go-jump-consistent-hash v1.0.2
	github.com/nats-io/nats.go v1.33.1
	github.com/spf13/cobra v1.3.0
	github.com/spf13/viper v1.10.1
	github.com/stretchr/testify v1.8.0
	go.uber.org/fx v1.17.0
	go.uber.org/zap v1.21.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dlclark/regexp2 v1.4.1-0.20201116162257-a2a8dda75c91 // indirect
	github.com/dop251/goja v0.0.0-20220214123719-b09a6bfa842f // indirect
	github.com/fsnotify/fsnotify v1.5.1 // indirect
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/klauspost/compress v1.17.2 // indirect
	github.com/magiconair/properties v1.8.6 // indirect
	github.com/mitchellh/mapstructure v1.4.3 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/nats-io/nkeys v0.4.7 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/pelletier/go-toml v1.9.4 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spf13/afero v1.8.1 // indirect
	github.com/spf13/cast v1.4.1 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/subosito/gotenv v1.2.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/dig v1.14.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	golang.org/x/crypto v0.18.0 // indirect
	golang.org/x/sys v0.16.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/ini.v1 v1.66.4 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/BrobridgeOrg/gravity-sdk/v2 => ../gravity-sdk

// replace github.com/BrobridgeOrg/compton => ../../compton
