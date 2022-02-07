package dispatcher

import (
	"fmt"
	"sync"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/message"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/rule_manager"
	product_sdk "github.com/BrobridgeOrg/gravity-sdk/product"
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
	dispatcher   *Dispatcher
	dataProducts sync.Map
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

	pm.dataProducts.Store(name, p)

	return p
}

func (pm *ProductManager) DeleteProduct(name string) {

	v, ok := pm.dataProducts.LoadAndDelete(name)
	if !ok {
		return
	}

	p := v.(*Product)
	p.StopEventWatcher()
}

func (pm *ProductManager) GetProduct(name string) *Product {

	v, ok := pm.dataProducts.Load(name)
	if !ok {
		return nil
	}

	return v.(*Product)
}

func (pm *ProductManager) ApplySettings(name string, setting *product_sdk.ProductSetting) error {

	v, ok := pm.dataProducts.Load(name)
	if ok {
		// New dataProduct
		p := pm.CreateProduct(name)
		p.Enabled = setting.Enabled
		p.ApplyRules(setting.Rules)
		return p.StartEventWatcher()
	}

	// Apply new rules
	p := v.(*Product)
	p.Enabled = setting.Enabled
	p.ApplyRules(setting.Rules)

	return nil
}

type Product struct {
	ID      string
	Name    string
	Rules   *rule_manager.RuleManager
	Enabled bool

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
		fmt.Sprintf("GRAVITY.%s.DP.%s", connector.GetDomain(), p.Name),
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

	m := message.New()
	m.Msg = msg
	m.Rule = rules[0]
	m.Raw = msg.Data

	p.manager.dispatcher.processor.Push(m)
}

func (p *Product) ApplyRules(ruleSet *product_sdk.RuleSet) error {

	// Preparing new rules
	rm := rule_manager.NewRuleManager()
	for _, r := range ruleSet.List() {
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
