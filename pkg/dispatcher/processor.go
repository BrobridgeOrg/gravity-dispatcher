package dispatcher

import (
	"fmt"
	"sync"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/converter"
	gravity_sdk_types_product_event "github.com/BrobridgeOrg/gravity-sdk/v2/types/product_event"
	record_type "github.com/BrobridgeOrg/gravity-sdk/v2/types/record"
	sdf "github.com/BrobridgeOrg/sequential-data-flow"
	"github.com/lithammer/go-jump-consistent-hash"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

var productEventPool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_product_event.ProductEvent{}
	},
}

var natsMsgPool = sync.Pool{
	New: func() interface{} {
		return &nats.Msg{}
	},
}

type Processor struct {
	flow          *sdf.Flow
	outputHandler func(*Message)
	domain        string
}

func NewProcessor(opts ...func(*Processor)) *Processor {

	p := &Processor{
		outputHandler: func(*Message) {},
	}

	// Apply options
	for _, o := range opts {
		o(p)
	}

	// Initialize sequential data flow
	options := sdf.NewOptions()
	options.BufferSize = 20480
	options.Handler = func(data interface{}, output func(interface{})) {
		p.handle(data.(*Message), output)
	}
	p.flow = sdf.NewFlow(options)

	go func() {
		for result := range p.flow.Output() {
			p.outputHandler(result.(*Message))
		}
	}()

	return p
}

func WithDomain(domain string) func(*Processor) {
	return func(p *Processor) {
		p.domain = domain
	}
}

func WithOutputHandler(fn func(*Message)) func(*Processor) {
	return func(p *Processor) {
		p.outputHandler = fn
	}
}

func (p *Processor) Push(msg *Message) {
	p.flow.Push(msg)
}

func (p *Processor) Close() {
	p.flow.Close()
}

func (p *Processor) handle(msg *Message, output func(interface{})) {

	if msg.Ignore {
		output(msg)
		return
	}

	if msg.Rule == nil {
		if !p.checkRule(msg) {
			// No match found, so ignore
			msg.Ignore = true
			output(msg)
			return
		}
	}

	// Parsing raw data
	err := msg.ParseRawData()
	if err != nil {
		logger.Error("Failed to parse message",
			zap.Error(err),
		)
		msg.Ignore = true
		output(msg)
		return
	}

	//	p.calculatePrimaryKey(msg)

	// Mapping and convert raw data to product_event object
	product_event, err := p.convert(msg)
	if err != nil {
		// Failed to parse payload
		logger.Error(err.Error())
		msg.Ignore = true
		output(msg)
		return
	}

	msg.ProductEvent = product_event

	// Convert product_event to bytes
	rawProductEvent, _ := gravity_sdk_types_product_event.Marshal(product_event)
	msg.RawProductEvent = rawProductEvent

	// Only avaialble if NATS message object exists
	var header nats.Header
	if msg.Msg != nil {
		// Unique message ID
		meta, _ := msg.Msg.Metadata()
		msg.ID = fmt.Sprintf("%d", meta.Sequence.Consumer)
		header = msg.Msg.Header
	}

	// Output subject
	subject := fmt.Sprintf("$GVT.%s.DP.%s.%d.EVENT.%s",
		p.domain,
		msg.ProductEvent.Table,
		msg.Partition,
		msg.ProductEvent.EventName,
	)

	// Prepare result object
	msg.OutputMsg = natsMsgPool.Get().(*nats.Msg)
	msg.OutputMsg.Subject = subject
	msg.OutputMsg.Data = rawProductEvent
	msg.OutputMsg.Header = header
	/*
		msg.OutputMsg = &nats.Msg{
			Subject: subject,
			Data:    rawProductEvent,
			Header:  header,
		}
	*/

	// Calculate partion based on primary key
	p.calculatePartition(msg)

	output(msg)
}

func (p *Processor) checkRule(msg *Message) bool {

	// Get matched rules by event
	rules := msg.Product.Rules.GetRulesByEvent(msg.Event)
	if len(rules) == 0 {
		logger.Warn("Ignore event",
			zap.String("event", msg.Event),
		)
		return false
	}

	msg.Rule = rules[0]

	return true
}

/*
func (p *Processor) calculatePrimaryKey(msg *Message) {

		// Getting normalized product_event from map object
		product_event := msg.Rule.Schema.Scan(msg.Data.Payload)

		// Collect values from fields to calculate primary key
		primaryKeys := make([]string, len(msg.Rule.PrimaryKey))
		for i, pk := range msg.Rule.PrimaryKey {
			v := product_event.GetValue(pk)
			primaryKeys[i] = fmt.Sprintf("%v", v.Data)
		}

		// Merge all values
		pk := strings.Join(primaryKeys, "-")
		msg.Data.PrimaryKey = StrToBytes(pk)
	}
*/
func (p *Processor) calculatePartition(msg *Message) {
	msg.Partition = jump.HashString(string(msg.ProductEvent.PrimaryKey), 256, jump.NewCRC64())
}

func (p *Processor) convert(msg *Message) (*gravity_sdk_types_product_event.ProductEvent, error) {

	// Prepare product_event
	pe := productEventPool.Get().(*gravity_sdk_types_product_event.ProductEvent)
	pe.EventName = msg.Data.Event
	pe.Method = gravity_sdk_types_product_event.Method(gravity_sdk_types_product_event.Method_value[msg.Rule.Method])
	pe.Table = msg.Rule.Product
	pe.PrimaryKeys = msg.Rule.PrimaryKey

	// Transforming
	results, err := msg.Rule.Handler.Run(nil, msg.Data.Payload)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		// Ignore
		return nil, nil
	}

	// Fill product_event
	result := results[0]
	fields, err := converter.Convert(msg.Rule.Handler.Transformer.GetDestinationSchema(), result)
	if err != nil {
		return nil, err
	}

	r := record_type.NewRecord()
	r.Payload.Map.Fields = fields

	// Calcuate primary key
	pk, err := r.CalculateKey(pe.PrimaryKeys)
	if err != nil {
		return nil, err
	}

	pe.PrimaryKey = pk

	// Write data back to product event
	pe.SetContent(r)

	//TODO: reuse record object

	return pe, nil
}
