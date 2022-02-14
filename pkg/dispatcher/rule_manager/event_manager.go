package rule_manager

type EventManager struct {
	events map[string]*RuleSet
}

func NewEventManager() *EventManager {
	return &EventManager{
		events: make(map[string]*RuleSet),
	}
}

func (em *EventManager) AddRule(eventName string, rule *Rule) {

	rs := em.GetRuleSet(eventName)
	if rs != nil {
		rs.Set(rule.ID, rule)
		return
	}

	rs = NewRuleSet()
	rs.Set(rule.ID, rule)

	em.events[eventName] = rs
}

func (em *EventManager) DeleteRule(eventName string, ruleID string) {

	rs := em.GetRuleSet(eventName)
	if rs == nil {
		return
	}

	rs.Delete(ruleID)
}

func (em *EventManager) GetRuleSet(eventName string) *RuleSet {

	if v, ok := em.events[eventName]; ok {
		return v
	}

	return nil
}

func (em *EventManager) GetEvents() []string {

	events := make([]string, len(em.events))

	for event, _ := range em.events {
		events = append(events, event)
	}

	return events
}
