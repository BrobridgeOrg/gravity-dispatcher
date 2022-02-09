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

func (pm *ProductManager) CreateProduct(name string) *Product {

	p := NewProduct(pm)
	p.Name = name

	// Generate ID
	id, _ := uuid.NewUUID()
	p.ID = id.String()

	p.init()

	pm.products.Store(name, p)

	return p
}

func (pm *ProductManager) DeleteProduct(name string) {

	v, ok := pm.products.LoadAndDelete(name)
	if !ok {
		return
	}

	p := v.(*Product)
	p.StopEventWatcher()
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
		// New dataProduct
		p := pm.CreateProduct(name)
		return p.ApplySettings(setting)
	}

	// Apply new settings
	p := v.(*Product)
	return p.ApplySettings(setting)
}

type Product struct {
	ID      string
	Name    string
	Enabled bool
	Rules   *rule_manager.RuleManager
	Schema  *schemer.Schema

	manager *ProductManager
	watcher *EventWatcher
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
		fmt.Sprintf("GRAVITY_%s_DP_%s", connector.GetDomain(), p.Name),
	)

	err := p.watcher.Init()
	if err != nil {
		return err
	}

	return nil
}

func (p *Product) handleMessage(eventName string, msg *nats.Msg) {

	// Get rules by event
	rules := p.Rules.GetRulesByEvent(eventName)
	if len(rules) == 0 {
		logger.Warn("Ignore event",
			zap.String("event", eventName),
		)
		return
	}

	// Getting message sequence
	//meta, _ := msg.Metadata()
	//fmt.Println(meta.Sequence.Consumer)

	m := NewMessage()
	m.Msg = msg
	m.Rule = rules[0]
	m.Product = p
	m.Raw = msg.Data

	p.manager.dispatcher.processor.Push(m)
}

func (p *Product) ApplySettings(setting *product_sdk.ProductSetting) error {

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
		rm.AddRule(rule)
	}

	p.Rules = rm

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
	return p.watcher.Watch(p.handleMessage)
}

func (p *Product) StopEventWatcher() error {
	return p.watcher.Stop()
}
