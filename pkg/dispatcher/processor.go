package dispatcher

import (
	"fmt"
	"strings"
	"sync"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/converter"
	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	sdf "github.com/BrobridgeOrg/sequential-data-flow"
	"github.com/lithammer/go-jump-consistent-hash"
	"go.uber.org/zap"
)

var recordPool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_record.Record{}
	},
}

type Processor struct {
	flow          *sdf.Flow
	outputHandler func(*Message)
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
	options.BufferSize = 10240
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

func WithOutputHandler(fn func(*Message)) func(*Processor) {
	return func(p *Processor) {
		p.outputHandler = fn
	}
}

func (p *Processor) Push(msg *Message) {
	p.flow.Push(msg)
}

func (p *Processor) handle(msg *Message, output func(interface{})) {

	// Parsing raw data
	err := msg.ParseRawData()
	if err != nil {
		logger.Error("Failed to parse message",
			zap.Error(err),
		)
		return
	}

	p.calculatePrimaryKey(msg)
	p.calculatePartition(msg)

	// Mapping and convert raw data to record object
	record, err := p.convert(msg)
	if err != nil {
		// Failed to parse payload
		logger.Error(err.Error())
		return
	}

	msg.Record = record

	rawRecord, _ := gravity_sdk_types_record.Marshal(record)
	msg.RawRecord = rawRecord

	output(msg)
}

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

func (p *Processor) calculatePartition(msg *Message) {
	msg.Partition = jump.HashString(string(msg.Data.PrimaryKey), 256, jump.NewCRC64())
}

func (p *Processor) convert(msg *Message) (*gravity_sdk_types_record.Record, error) {

	// Prepare record
	record := recordPool.Get().(*gravity_sdk_types_record.Record)
	record.EventName = msg.Data.Event
	record.Method = gravity_sdk_types_record.Method(gravity_sdk_types_record.Method_value[msg.Rule.Method])
	record.Table = msg.Rule.Product
	record.PrimaryKey = string(msg.Data.PrimaryKey)

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

	return record, nil
}
