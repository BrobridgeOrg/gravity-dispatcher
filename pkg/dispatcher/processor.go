package dispatcher

import (
	"fmt"
	"strings"
	"sync"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/converter"
	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/message"
	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	sdf "github.com/BrobridgeOrg/sequential-data-flow"
	"github.com/lithammer/go-jump-consistent-hash"
)

var recordPool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_record.Record{}
	},
}

type Processor struct {
	flow          *sdf.Flow
	outputHandler func(*message.Message)
}

func NewProcessor(opts ...func(*Processor)) *Processor {

	p := &Processor{
		outputHandler: func(*message.Message) {},
	}

	// Apply options
	for _, o := range opts {
		o(p)
	}

	// Initialize sequential data flow
	options := sdf.NewOptions()
	options.BufferSize = 1024
	options.Handler = func(data interface{}, output func(interface{})) {
		p.handle(data.(*message.Message), output)
	}
	p.flow = sdf.NewFlow(options)

	go func() {
		for result := range p.flow.Output() {
			p.outputHandler(result.(*message.Message))
		}
	}()

	return p
}

func WithOutputHandler(fn func(*message.Message)) func(*Processor) {
	return func(p *Processor) {
		p.outputHandler = fn
	}
}

func (p *Processor) Push(msg *message.Message) {
	p.flow.Push(msg)
}

func (p *Processor) handle(msg *message.Message, output func(interface{})) {

	// Parsing raw data
	err := msg.ParseRawData()
	if err != nil {
		logger.Error(err.Error())
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

func (p *Processor) calculatePrimaryKey(msg *message.Message) {

	// Getting normalized record from map object
	record := msg.Rule.Schema.Scan(msg.Data.Payload)

	// Collect values from fields to calculate primary key
	primaryKeys := make([]string, 0)
	for _, pk := range msg.Rule.PrimaryKey {
		v := record.GetValue(pk)
		primaryKeys = append(primaryKeys, fmt.Sprintf("%v", v.Data))
	}

	// Merge all values
	pk := strings.Join(primaryKeys, "-")
	msg.Data.PrimaryKey = []byte(pk)
}

func (p *Processor) calculatePartition(msg *message.Message) {
	msg.Partition = jump.HashString(string(msg.Data.PrimaryKey), 256, jump.NewCRC64())
}

func (p *Processor) convert(msg *message.Message) (*gravity_sdk_types_record.Record, error) {

	// Prepare record
	record := recordPool.Get().(*gravity_sdk_types_record.Record)
	record.EventName = msg.Data.Event
	record.Method = gravity_sdk_types_record.Method(gravity_sdk_types_record.Method_value[msg.Rule.Method])
	record.Table = msg.Rule.DataProduct
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
