package rule_manager

type RuleSet struct {
	rules map[string]*Rule
}

func NewRuleSet() *RuleSet {
	return &RuleSet{
		rules: make(map[string]*Rule),
	}
}

func (rm *RuleSet) Set(id string, rule *Rule) {
	rm.rules[id] = rule
}

func (rm *RuleSet) Delete(id string) {
	delete(rm.rules, id)
}

func (rm *RuleSet) Get(id string) *Rule {

	if v, ok := rm.rules[id]; ok {
		return v
	}

	return nil
}

func (rm *RuleSet) List() []*Rule {

	rules := make([]*Rule, 0, len(rm.rules))

	for _, rule := range rm.rules {
		rules = append(rules, rule)
	}

	return rules
}
