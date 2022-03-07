package dispatcher

import (
	"fmt"
	"sync"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/converter"
	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	sdf "github.com/BrobridgeOrg/sequential-data-flow"
	"github.com/lithammer/go-jump-consistent-hash"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

var recordPool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_record.Record{}
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

	if msg.Rule == nil {
		if !p.checkRule(msg) {
			// No match found, so ignore
			return
		}
	}

	// Parsing raw data
	err := msg.ParseRawData()
	if err != nil {
		logger.Error("Failed to parse message",
			zap.Error(err),
		)
		return
	}

	//	p.calculatePrimaryKey(msg)

	// Mapping and convert raw data to record object
	record, err := p.convert(msg)
	if err != nil {
		// Failed to parse payload
		logger.Error(err.Error())
		return
	}

	msg.Record = record

	// Convert record to bytes
	rawRecord, _ := gravity_sdk_types_record.Marshal(record)
	msg.RawRecord = rawRecord

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
		msg.Record.Table,
		msg.Partition,
		msg.Record.EventName,
	)

	// Prepare result object
	msg.OutputMsg = natsMsgPool.Get().(*nats.Msg)
	msg.OutputMsg.Subject = subject
	msg.OutputMsg.Data = rawRecord
	msg.OutputMsg.Header = header
	/*
		msg.OutputMsg = &nats.Msg{
			Subject: subject,
			Data:    rawRecord,
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

	// Getting normalized record from map object
	record := msg.Rule.Schema.Scan(msg.Data.Payload)

	// Collect values from fields to calculate primary key
	primaryKeys := make([]string, len(msg.Rule.PrimaryKey))
	for i, pk := range msg.Rule.PrimaryKey {
		v := record.GetValue(pk)
		primaryKeys[i] = fmt.Sprintf("%v", v.Data)
	}

	// Merge all values
	pk := strings.Join(primaryKeys, "-")
	msg.Data.PrimaryKey = StrToBytes(pk)
}
*/
func (p *Processor) calculatePartition(msg *Message) {
	msg.Partition = jump.HashString(string(msg.Record.PrimaryKey), 256, jump.NewCRC64())
}

func (p *Processor) convert(msg *Message) (*gravity_sdk_types_record.Record, error) {

	// Prepare record
	record := recordPool.Get().(*gravity_sdk_types_record.Record)
	record.EventName = msg.Data.Event
	record.Method = gravity_sdk_types_record.Method(gravity_sdk_types_record.Method_value[msg.Rule.Method])
	record.Table = msg.Rule.Product
	record.PrimaryKeys = msg.Rule.PrimaryKey

	// Transforming
	results, err := msg.Rule.Handler.Run(nil, msg.Data.Payload)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		// Ignore
		return nil, nil
	}

	// Fill record
	result := results[0]
	fields, err := converter.Convert(msg.Rule.Handler.Transformer.GetDestinationSchema(), result)
	if err != nil {
		return nil, err
	}

	record.Fields = fields

	// Calcuate primary key
	pk, err := record.GetPrimaryKeyData()
	if err != nil {
		return nil, err
	}

	record.PrimaryKey = pk

	return record, nil
}
