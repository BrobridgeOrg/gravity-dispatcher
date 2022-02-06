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

type CollectionSetting struct {
	Name        string                `json:"name"`
	Description string                `json:"desc"`
	Rules       *rule_manager.RuleSet `json:"rules"`
}

type CollectionManager struct {
	dispatcher  *Dispatcher
	collections sync.Map
}

func NewCollectionManager(d *Dispatcher) *CollectionManager {
	return &CollectionManager{
		dispatcher: d,
	}
}

func (cm *CollectionManager) CreateCollection(name string) *Collection {

	c := NewCollection(cm)
	c.Name = name

	// Generate ID
	id, _ := uuid.NewUUID()
	c.ID = id.String()

	cm.collections.Store(name, c)

	return c
}

func (cm *CollectionManager) DeleteCollection(name string) {

	v, ok := cm.collections.LoadAndDelete(name)
	if !ok {
		return
	}

	c := v.(*Collection)
	c.StopEventWatcher()
}

func (cm *CollectionManager) GetCollection(name string) *Collection {

	c, ok := cm.collections.Load(name)
	if !ok {
		return nil
	}

	return c.(*Collection)
}

func (cm *CollectionManager) ApplySettings(name string, setting *CollectionSetting) error {

	v, ok := cm.collections.Load(name)
	if ok {
		// New collection
		c := cm.CreateCollection(name)
		c.ApplyRules(setting.Rules)
		return c.StartEventWatcher()
	}

	// Apply new rules
	c := v.(*Collection)
	c.ApplyRules(setting.Rules)

	return nil
}

type Collection struct {
	ID    string
	Name  string
	Rules *rule_manager.RuleManager

	manager *CollectionManager
	watcher *EventWatcher
}

func NewCollection(cm *CollectionManager) *Collection {
	return &Collection{
		Rules:   rule_manager.NewRuleManager(),
		manager: cm,
	}
}

func (c *Collection) handleMessage(eventName string, msg *nats.Msg) {

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

func (c *Collection) ApplyRules(ruleSet *rule_manager.RuleSet) error {

	// Preparing new rules
	rm := rule_manager.NewRuleManager()
	for _, r := range ruleSet.List() {
		rm.AddRule(r)
	}

	c.Rules = rm

	return nil
}

func (c *Collection) StartEventWatcher() error {

	connector := c.manager.dispatcher.connector

	// Initializing event watcher
	c.watcher = NewEventWatcher(
		connector.GetClient(),
		connector.GetDomain(),
		fmt.Sprintf("GRAVITY.%s.COLLECTION.%s", connector.GetDomain(), c.Name),
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

func (c *Collection) StopEventWatcher() error {
	return c.watcher.Stop()
}
