package dispatcher

import (
	"fmt"
	"sync"
	"time"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/rule_manager"
	product_sdk "github.com/BrobridgeOrg/gravity-sdk/v2/product"
	"github.com/BrobridgeOrg/schemer"
	buffered_input "github.com/cfsghost/buffered-input"
	"github.com/google/uuid"
	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const (
	DefaultProductMaxFlushInterval = 100 * time.Millisecond
	DefaultProductMaxStreamBytes   = 8 * 1024 * 1024 * 1024 // 8GB
	DefaultProductMaxStreamAge     = 7 * 24 * time.Hour     // 1 week
	DefaultProductDuplicates       = 5 * time.Minute        // 5 minutes
)

const (
	productEventStream  = "GVT_%s_DP_%s"
	productEventSubject = "$GVT.%s.DP.%s.*.EVENT.>"
	domainEventConsumer = "GVT_%s_DP_%s"
)

type ProductSetting struct {
	Name        string                `json:"name"`
	Description string                `json:"desc"`
	Enabled     bool                  `json:"enabled"`
	Rules       *rule_manager.RuleSet `json:"rules"`
}

type ProductManager struct {
	dispatcher *Dispatcher
	products   sync.Map
}

func NewProductManager(d *Dispatcher) *ProductManager {
	return &ProductManager{
		dispatcher: d,
	}
}

func (pm *ProductManager) assertProductStream(name string, streamName string) error {

	viper.SetDefault("product.max_stream_bytes", DefaultProductMaxStreamBytes)
	viper.SetDefault("product.max_stream_age", DefaultProductMaxStreamAge)
	viper.SetDefault("product.duplicates", DefaultProductDuplicates)

	maxStreamBytes := viper.GetInt64("product.max_stream_bytes")
	maxStreamAge := viper.GetDuration("product.max_stream_age")
	duplicates := viper.GetDuration("product.duplicates")

	// Validate maxStreamBytes
	if maxStreamAge <= 0 {
		maxStreamAge = 0
	}

	// Preparing JetStream
	js, err := pm.dispatcher.connector.GetClient().GetJetStream()
	if err != nil {
		return err
	}

	if len(streamName) == 0 {
		streamName = fmt.Sprintf(productEventStream, pm.dispatcher.connector.GetDomain(), name)
	}

	logger.Info("Checking product stream",
		zap.String("product", name),
		zap.String("stream", streamName),
	)

	// Check if the stream already exists
	stream, err := js.StreamInfo(streamName)
	if err != nil {
		if err != nats.ErrStreamNotFound {
			return err
		}

		logger.Warn("Product stream is not ready",
			zap.String("product", name),
			zap.String("stream", streamName),
		)
	}

	if stream == nil {

		// Event subject
		subject := fmt.Sprintf(productEventSubject, pm.dispatcher.connector.GetDomain(), name)

		// Initializing stream
		logger.Info("Creating a new product stream...",
			zap.String("product", name),
			zap.String("stream", streamName),
			zap.String("subject", subject),
			zap.Int64("max_stream_bytes", maxStreamBytes),
			zap.Duration("max_stream_age", maxStreamAge),
			zap.Duration("duplicates", duplicates),
		)

		sc := &nats.StreamConfig{
			Name:        streamName,
			Description: "Gravity product event store",
			Duplicates:  duplicates,
			Subjects: []string{
				subject,
			},
			Retention:   nats.LimitsPolicy,
			MaxBytes:    maxStreamBytes,
			MaxAge:      maxStreamAge,
			Compression: nats.S2Compression,
			Replicas:    3,
		}

		_, err := js.AddStream(sc)
		if err != nil {

			// for single node
			sc.Replicas = 1
			_, err := js.AddStream(sc)
			if err != nil {
				return err
			}
		}
	}

	return nil

}

func (pm *ProductManager) CreateProduct(name string, streamName string) *Product {

	// Assert product stream
	err := pm.assertProductStream(name, streamName)
	if err != nil {
		logger.Error("Failed to create product stream",
			zap.Error(err),
		)

		return nil
	}

	p := NewProduct(pm)
	p.Domain = pm.dispatcher.connector.GetDomain()
	p.Name = name

	// Generate ID
	id, _ := uuid.NewUUID()
	p.ID = id.String()

	p.init()

	pm.products.Store(name, p)

	return p
}

func (pm *ProductManager) DeleteProduct(name string) error {

	v, ok := pm.products.LoadAndDelete(name)
	if !ok {
		return nil
	}

	p := v.(*Product)
	p.StopEventWatcher()

	js, err := pm.dispatcher.connector.GetClient().GetJetStream()
	if err != nil {
		return err
	}

	streamName := fmt.Sprintf(productEventStream, pm.dispatcher.connector.GetDomain(), name)
	err = js.DeleteStream(streamName)
	if err != nil {
		logger.Warn("Failed to delete product stream",
			zap.Error(err),
		)

		return nil
	}

	return nil
}

func (pm *ProductManager) GetProduct(name string) *Product {

	v, ok := pm.products.Load(name)
	if !ok {
		return nil
	}

	return v.(*Product)
}

func (pm *ProductManager) ApplySettings(name string, setting *product_sdk.ProductSetting) error {

	ruleCount := 0
	if setting.Rules != nil {
		ruleCount = len(setting.Rules)
	}

	logger.Info("Applying product settings",
		zap.String("name", name),
		zap.Bool("enabled", setting.Enabled),
		zap.Int("ruleCount", ruleCount),
	)

	v, ok := pm.products.Load(name)
	if !ok {

		logger.Info("Create product",
			zap.String("product", name),
		)

		// New dataProduct
		p := pm.CreateProduct(name, setting.Stream)

		return p.ApplySettings(setting)
	}

	logger.Info("Update product",
		zap.String("product", name),
	)

	// Apply new settings
	p := v.(*Product)
	return p.ApplySettings(setting)
}

type Product struct {
	ID        string
	Domain    string
	Name      string
	Enabled   bool
	Rules     *rule_manager.RuleManager
	Schema    *schemer.Schema
	IsRunning bool

	processor        *Processor
	dispatcherBuffer *buffered_input.BufferedInput
	manager          *ProductManager
	watcher          *EventWatcher
	onMessage        func(msg *Message)
}

func NewProduct(pm *ProductManager) *Product {

	p := &Product{
		Rules:   rule_manager.NewRuleManager(),
		manager: pm,
	}

	p.reset()
	p.onMessage = p.dispatch

	return p
}

func (p *Product) initDispatcherBuffer() {

	viper.SetDefault("product.max_flush_interval", DefaultProductMaxFlushInterval)

	maxFlushInterval := viper.GetDuration("product.max_flush_interval")

	logger.Info("Initializing dispatcher buffer",
		zap.String("product", p.Name),
		zap.Duration("max_flush_interval", maxFlushInterval),
	)

	// Initializing buffered input
	opts := buffered_input.NewOptions()
	opts.ChunkSize = 2000
	opts.ChunkCount = 1000
	opts.Timeout = maxFlushInterval
	opts.Handler = p.dispatcherBufferHandler
	p.dispatcherBuffer = buffered_input.NewBufferedInput(opts)
}

func (p *Product) reset() {

	p.initDispatcherBuffer()

	// Initializing processer
	p.processor = NewProcessor(
		WithDomain(p.Domain),
		WithOutputHandler(p.emit),
	)
}

func (p *Product) getConnector() *connector.Connector {
	return p.manager.dispatcher.connector
}

func (p *Product) init() error {

	connector := p.getConnector()

	// Initializing event watcher
	p.watcher = NewEventWatcher(
		connector.GetClient(),
		p.Domain,
		fmt.Sprintf(domainEventConsumer, connector.GetDomain(), p.Name),
	)

	err := p.watcher.Init()
	if err != nil {
		return err
	}

	return nil
}

func (p *Product) emit(msg *Message) {
	p.onMessage(msg)
}

func (p *Product) dispatcherBufferHandler(chunk []interface{}) {

	doneCount := 0
	var prev *Message
	msgs := make([]*Message, 0)
	for i, v := range chunk {

		if !p.IsRunning {
			return
		}

		m := v.(*Message)

		/*
			fmt.Println("Dispatching message", m.Event)
			fmt.Println(string(m.Data.RawPayload))
			fmt.Println(m.Data.Payload)
		*/
		err := m.Dispatch()
		if err != nil {

			// Ack for the last message
			if prev != nil {
				prev.Ack()
				prev.Release()
				doneCount = i

				logger.Info("Messages were dispatched",
					zap.String("product", p.Name),
					zap.Int("count", i),
				)
			}

			logger.Error("Failed to dispatch",
				zap.Error(err),
			)

			// Retry
			for {
				time.Sleep(time.Second)

				if !p.IsRunning {
					return
				}

				logger.Info("Retrying to ack",
					zap.String("product", p.Name),
				)

				err := m.Dispatch()
				if err == nil {
					// Success
					break
				}
			}
		}

		msgs = append(msgs, m)
		prev = m
	}

	// Wait for all messages to be dispatched
	for _, m := range msgs {
		err := m.Wait()
		if err != nil {
			logger.Error("Failed to wait for ack",
				zap.Error(err),
			)
		}
	}

	if doneCount < len(chunk) {
		prev.Ack()
		prev.Release()

		logger.Info("Messages were dispatched",
			zap.String("product", p.Name),
			zap.Int("count", len(chunk)-doneCount),
		)
	}
}

func (p *Product) dispatch(msg *Message) {
	p.dispatcherBuffer.Push(msg)
}

func (p *Product) handleMessage(eventName string, msg *nats.Msg) {

	data := msg.Data

	// Decompress message
	if msg.Header.Get("Content-Encoding") == "s2" {
		decompressedMessage, err := s2.Decode(nil, msg.Data)
		if err != nil {
			logger.Error("Failed to decompress message",
				zap.Error(err),
			)

			return
		}

		data = decompressedMessage
	}

	m := NewMessage()
	m.Publisher = p.manager.dispatcher.publisherJSCtx
	m.Event = eventName
	m.Msg = msg
	m.Product = p
	m.Raw = data

	if len(eventName) > 0 {
		m.Ignore = false
	} else {
		m.Ignore = true
	}

	p.processor.Push(m)
}

func (p *Product) HandleRawMessage(eventName string, raw []byte) {
	m := NewMessage()
	m.Ignore = false
	m.Event = eventName
	m.Product = p
	m.Raw = raw

	p.processor.Push(m)
}

func (p *Product) ApplySettings(setting *product_sdk.ProductSetting) error {

	err := p.deactivate()
	if err != nil {
		return err
	}

	p.PurgeTasks()

	p.Name = setting.Name
	p.Enabled = setting.Enabled

	// Product schema
	if setting.Schema != nil {
		p.Schema = schemer.NewSchema()
		err := schemer.Unmarshal(setting.Schema, p.Schema)
		if err != nil {
			return err
		}
	}

	//TODO: do nothing if only snapshot settings was changed

	// Apply new rules
	rules := make([]*product_sdk.Rule, 0)
	for _, rule := range setting.Rules {
		rules = append(rules, rule)
	}
	p.ApplyRules(rules)

	err = p.Activate()
	if err != nil {
		return err
	}

	return nil
}

func (p *Product) ApplyRules(rules []*product_sdk.Rule) error {

	// Preparing new rules
	rm := rule_manager.NewRuleManager()

	for _, r := range rules {
		rule := rule_manager.NewRule(r)
		rule.TargetSchema = p.Schema
		rm.AddRule(rule)
	}

	// Replace old rule manager
	p.Rules = rm

	if p.watcher == nil {
		return nil
	}

	// Purge events
	p.watcher.PurgeEvent()

	// Registering events
	events := p.Rules.GetEvents()
	for _, event := range events {
		p.watcher.RegisterEvent(event)
	}

	return nil
}

func (p *Product) PurgeTasks() error {

	if p.dispatcherBuffer == nil {
		return nil
	}

	p.dispatcherBuffer.Close()

	if p.processor == nil {
		return nil
	}

	p.processor.Close()

	p.reset()

	return nil
}

func (p *Product) deactivate() error {

	p.IsRunning = false

	// Stop receiving events
	err := p.StopEventWatcher()
	if err != nil {
		return err
	}

	return nil
}

func (p *Product) Deactivate() error {

	logger.Info("Deactivating product",
		zap.String("product", p.Name),
	)

	return p.deactivate()
}

func (p *Product) Activate() error {

	// Product is disabled
	if !p.Enabled {
		return nil
	}

	p.IsRunning = true

	logger.Info("Activating product",
		zap.String("product", p.Name),
	)

	err := p.StartEventWatcher()
	if err != nil {
		return err
	}

	return nil
}

func (p *Product) StartEventWatcher() error {

	if p.watcher == nil {
		return nil
	}

	return p.watcher.Watch(p.handleMessage)
}

func (p *Product) StopEventWatcher() error {

	if p.watcher == nil {
		return nil
	}

	return p.watcher.Stop()
}
