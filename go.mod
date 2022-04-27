module github.com/BrobridgeOrg/gravity-dispatcher

go 1.15

require (
	github.com/BrobridgeOrg/compton v0.0.0-20220315055918-5c0ee958b7d2
	github.com/BrobridgeOrg/gravity-sdk v0.0.50
	github.com/BrobridgeOrg/gravity-snapshot v0.0.0-20220213082459-fd38b9058d95
	github.com/BrobridgeOrg/schemer v0.0.10
	github.com/BrobridgeOrg/sequential-data-flow v0.0.1
	github.com/bytedance/sonic v1.1.1
	github.com/cfsghost/buffered-input v0.0.1
	github.com/chenzhuoyu/base64x v0.0.0-20211229061535-45e1f0233683 // indirect
	github.com/d5/tengo v1.24.8
	github.com/dop251/goja v0.0.0-20220214123719-b09a6bfa842f // indirect
	github.com/google/uuid v1.3.0
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/cpuid/v2 v2.0.11 // indirect
	github.com/lithammer/go-jump-consistent-hash v1.0.2
	github.com/magiconair/properties v1.8.6 // indirect
	github.com/nats-io/nats.go v1.13.1-0.20220121202836-972a071d373d
	github.com/spf13/afero v1.8.1 // indirect
	github.com/spf13/cobra v1.3.0
	github.com/spf13/viper v1.10.1
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/fx v1.17.0
	go.uber.org/multierr v1.8.0 // indirect
	go.uber.org/zap v1.21.0
	golang.org/x/crypto v0.0.0-20220307211146-efcb8507fb70 // indirect
	golang.org/x/sys v0.0.0-20220307203707-22a9840ba4d7 // indirect
	gopkg.in/ini.v1 v1.66.4 // indirect
)

replace github.com/BrobridgeOrg/gravity-sdk => ../gravity-sdk

replace github.com/BrobridgeOrg/compton => ../../compton
