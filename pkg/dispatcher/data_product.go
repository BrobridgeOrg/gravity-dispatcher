package dispatcher

import (
	"fmt"
	"sync"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/message"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/rule_manager"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type DataProductSetting struct {
	Name        string                `json:"name"`
	Description string                `json:"desc"`
	Rules       *rule_manager.RuleSet `json:"rules"`
}

type DataProductManager struct {
	dispatcher   *Dispatcher
	dataProducts sync.Map
}

func NewDataProductManager(d *Dispatcher) *DataProductManager {
	return &DataProductManager{
		dispatcher: d,
	}
}

func (cm *DataProductManager) CreateDataProduct(name string) *DataProduct {

	c := NewDataProduct(cm)
	c.Name = name

	// Generate ID
	id, _ := uuid.NewUUID()
	c.ID = id.String()

	cm.dataProducts.Store(name, c)

	return c
}

func (cm *DataProductManager) DeleteDataProduct(name string) {

	v, ok := cm.dataProducts.LoadAndDelete(name)
	if !ok {
		return
	}

	c := v.(*DataProduct)
	c.StopEventWatcher()
}

func (cm *DataProductManager) GetDataProduct(name string) *DataProduct {

	c, ok := cm.dataProducts.Load(name)
	if !ok {
		return nil
	}

	return c.(*DataProduct)
}

func (cm *DataProductManager) ApplySettings(name string, setting *DataProductSetting) error {

	v, ok := cm.dataProducts.Load(name)
	if ok {
		// New dataProduct
		c := cm.CreateDataProduct(name)
		c.ApplyRules(setting.Rules)
		return c.StartEventWatcher()
	}

	// Apply new rules
	c := v.(*DataProduct)
	c.ApplyRules(setting.Rules)

	return nil
}

type DataProduct struct {
	ID    string
	Name  string
	Rules *rule_manager.RuleManager

	manager *DataProductManager
	watcher *EventWatcher
}

func NewDataProduct(cm *DataProductManager) *DataProduct {
	return &DataProduct{
		Rules:   rule_manager.NewRuleManager(),
		manager: cm,
	}
}

func (c *DataProduct) handleMessage(eventName string, msg *nats.Msg) {

	// Get rules by event
	rules := c.Rules.GetRulesByEvent(eventName)
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

	c.manager.dispatcher.processor.Push(m)
}

func (c *DataProduct) ApplyRules(ruleSet *rule_manager.RuleSet) error {

	// Preparing new rules
	rm := rule_manager.NewRuleManager()
	for _, r := range ruleSet.List() {
		rm.AddRule(r)
	}

	c.Rules = rm

	return nil
}

func (c *DataProduct) StartEventWatcher() error {

	connector := c.manager.dispatcher.connector

	// Initializing event watcher
	c.watcher = NewEventWatcher(
		connector.GetClient(),
		connector.GetDomain(),
		fmt.Sprintf("GRAVITY.%s.DP.%s", connector.GetDomain(), c.Name),
	)
	err := c.watcher.Init()
	if err != nil {
		return err
	}

	// Registering events
	events := c.Rules.GetEvents()
	for _, event := range events {
		c.watcher.RegisterEvent(event)
	}

	// Start watching
	c.watcher.Watch(c.handleMessage)

	return nil
}

func (c *DataProduct) StopEventWatcher() error {
	return c.watcher.Stop()
}
