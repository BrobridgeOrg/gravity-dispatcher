package dispatcher

import (
	"fmt"
	"sync"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/rule_manager"
	product_sdk "github.com/BrobridgeOrg/gravity-sdk/product"
	"github.com/BrobridgeOrg/schemer"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
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
	handler    func(*Product, string, *Message)
}

func NewProductManager(d *Dispatcher) *ProductManager {
	return &ProductManager{
		dispatcher: d,
	}
}

func (pm *ProductManager) assertProductStream(name string, streamName string) error {

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
		logger.Warn(err.Error())
	}

	// Event subject
	subject := fmt.Sprintf(productEventSubject, pm.dispatcher.connector.GetDomain(), name)

	if stream == nil {

		// Initializing stream
		logger.Info("Creating a new product stream...",
			zap.String("product", name),
			zap.String("stream", streamName),
			zap.String("subject", subject),
		)

		_, err := js.AddStream(&nats.StreamConfig{
			Name:        streamName,
			Description: "Gravity product event store",
			Subjects: []string{
				subject,
			},
			Retention: nats.LimitsPolicy,
		})

		if err != nil {
			return err
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
	p.Name = name

	// Generate ID
	id, _ := uuid.NewUUID()
	p.ID = id.String()

	p.Subscribe(func(eventName string, message *Message) {
		pm.handler(p, eventName, message)
	})

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

func (pm *ProductManager) Subscribe(fn func(*Product, string, *Message)) {
	pm.handler = fn
}

type Product struct {
	ID      string
	Name    string
	Enabled bool
	Rules   *rule_manager.RuleManager
	Schema  *schemer.Schema

	manager *ProductManager
	watcher *EventWatcher
	handler func(string, *Message)
}

func NewProduct(pm *ProductManager) *Product {
	return &Product{
		Rules:   rule_manager.NewRuleManager(),
		manager: pm,
	}
}

func (p *Product) init() error {

	connector := p.manager.dispatcher.connector

	// Initializing event watcher
	p.watcher = NewEventWatcher(
		connector.GetClient(),
		connector.GetDomain(),
		fmt.Sprintf(domainEventConsumer, connector.GetDomain(), p.Name),
	)

	err := p.watcher.Init()
	if err != nil {
		return err
	}

	return nil
}

func (p *Product) handleMessage(eventName string, msg *nats.Msg) {

	if p.handler == nil {
		return
	}

	// Getting message sequence
	//meta, _ := msg.Metadata()
	//fmt.Println(meta.Sequence.Consumer)

	m := NewMessage()
	m.Event = eventName
	m.Msg = msg
	m.Rule = nil
	m.Product = p
	m.Raw = msg.Data

	p.handler(eventName, m)
}

func (p *Product) HandleRawMessage(eventName string, raw []byte) {

	if p.handler == nil {
		return
	}

	m := NewMessage()
	m.Event = eventName
	m.Msg = nil
	m.Rule = nil
	m.Product = p
	m.Raw = raw

	p.handler(eventName, m)
}

func (p *Product) Subscribe(fn func(string, *Message)) {
	p.handler = fn
}

func (p *Product) ApplySettings(setting *product_sdk.ProductSetting) error {

	p.Name = setting.Name

	// Disable
	if p.Enabled && p.Enabled != setting.Enabled {
		err := p.StopEventWatcher()
		if err != nil {
			return err
		}

		p.Enabled = setting.Enabled
	}

	// Preparing product schema
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

	// Enable
	if !p.Enabled && p.Enabled != setting.Enabled {
		err := p.StartEventWatcher()
		if err != nil {
			return err
		}

		p.Enabled = setting.Enabled
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
