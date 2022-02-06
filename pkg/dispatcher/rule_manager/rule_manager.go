package rule_manager

import (
	"github.com/google/uuid"
)

type RuleManager struct {
	rules  *RuleSet
	events *EventManager
}

func NewRuleManager() *RuleManager {
	return &RuleManager{
		rules:  NewRuleSet(),
		events: NewEventManager(),
	}
}

func (rm *RuleManager) AddRule(rule *Rule) error {

	id, _ := uuid.NewUUID()
	rule.ID = id.String()

	err := rule.applyConfigs()
	if err != nil {
		return err
	}

	// Registering
	rm.rules.Set(rule.ID, rule)
	rm.events.AddRule(rule.Event, rule)

	return nil
}

func (rm *RuleManager) DeleteRule(id string) {

	rule := rm.rules.Get(id)
	if rule == nil {
		return
	}

	rm.events.DeleteRule(rule.Event, rule.ID)

	rm.rules.Delete(id)
}

func (rm *RuleManager) GetRule(id string) *Rule {
	return rm.rules.Get(id)
}

func (rm *RuleManager) GetRules() []*Rule {
	return rm.rules.List()
}

func (rm *RuleManager) GetRulesByEvent(eventName string) []*Rule {

	ruleSet := rm.events.GetRuleSet(eventName)
	if ruleSet == nil {
		return make([]*Rule, 0)
	}

	return ruleSet.List()
}

func (rm *RuleManager) GetEvents() []string {
	return rm.events.GetEvents()
}
