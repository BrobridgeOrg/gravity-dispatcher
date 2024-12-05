package dispatcher

import (
	"fmt"
	"hash"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/converter"
	gravity_sdk_types_product_event "github.com/BrobridgeOrg/gravity-sdk/v2/types/product_event"
	record_type "github.com/BrobridgeOrg/gravity-sdk/v2/types/record"
	sequential_task_runner "github.com/BrobridgeOrg/sequential-task-runner"
	"github.com/lithammer/go-jump-consistent-hash"
	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const (
	DefaultProcessorWorkerCount     = 8
	DefaultProcessorMaxPendingCount = 2048
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
	runner        *sequential_task_runner.Runner
	outputHandler func(*Message)
	domain        string
	hash          hash.Hash64
}

func NewProcessor(opts ...func(*Processor)) *Processor {

	p := &Processor{
		outputHandler: func(*Message) {},
		hash:          jump.NewCRC64(),
	}

	// Apply options
	for _, o := range opts {
		o(p)
	}

	viper.SetDefault("processor.worker_count", DefaultProcessorWorkerCount)
	viper.SetDefault("processor.max_pending_count", DefaultProcessorMaxPendingCount)

	workerCount := viper.GetInt("processor.worker_count")
	maxPendingCount := viper.GetInt("processor.max_pending_count")

	logger.Info("Initializing processor",
		zap.Int("worker_count", workerCount),
		zap.Int("max_pending_count", maxPendingCount),
	)

	// Check if cpu count is less than worker count
	if runtime.GOMAXPROCS(0) < workerCount {
		runtime.GOMAXPROCS(workerCount + 4)
		logger.Warn("Worker count is greater than maxprocs, so increase maxprocs",
			zap.Int("worker_count", workerCount),
			zap.Int("new_maxprocs", workerCount+4),
		)
	}

	// Initializing sequential task runner
	p.runner = sequential_task_runner.NewRunner(
		sequential_task_runner.WithWorkerCount(workerCount),
		sequential_task_runner.WithMaxPendingCount(maxPendingCount),
		sequential_task_runner.WithWorkerHandler(func(workerID int, task interface{}) interface{} {
			return p.process(task.(*Message))
		}),
	)

	// Configure output handler
	p.runner.Subscribe(func(result interface{}) {
		p.outputHandler(result.(*Message))
	})

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
	p.runner.AddTask(msg)
}

func (p *Processor) Close() {
	p.runner.Close()
}

func (p *Processor) process(msg *Message) *Message {

	if msg.Ignore {
		return msg
	}

	if msg.Rule == nil {
		if !p.checkRule(msg) {
			// No match found, so ignore
			msg.Ignore = true
			return msg
		}
	}

	// Parsing raw data
	err := msg.ParseRawData()
	if err != nil {
		logger.Error("Failed to parse message",
			zap.Error(err),
		)
		msg.Ignore = true
		return msg
	}

	//	p.calculatePrimaryKey(msg)

	// Mapping and convert raw data to product_event object
	product_event, err := p.convert(msg)
	if err != nil {
		// Failed to process payload
		logger.Error("Failed to process payload",
			zap.Error(err),
		)
		msg.Ignore = true
		return msg
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
		//		msg.ID = fmt.Sprintf("%d", meta.Sequence.Stream)
		msg.ID = strconv.FormatUint(meta.Sequence.Stream, 16)
		header = msg.Msg.Header
	}

	// Calculate partion based on primary key
	p.calculatePartition(msg)

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

	return msg
}

func (p *Processor) checkRule(msg *Message) bool {

	if msg.Product == nil {
		return false
	}

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
	msg.Partition = jump.HashString(BytesToString(msg.ProductEvent.PrimaryKey), 256, p.hash)
}

func (p *Processor) convert(msg *Message) (*gravity_sdk_types_product_event.ProductEvent, error) {

	// Prepare product_event
	pe := productEventPool.Get().(*gravity_sdk_types_product_event.ProductEvent)
	pe.Reset()
	pe.EventName = msg.Data.Event
	pe.Method = gravity_sdk_types_product_event.Method(gravity_sdk_types_product_event.Method_value[strings.ToUpper(msg.Rule.Method)])
	pe.Table = msg.Rule.Product
	pe.PrimaryKeys = msg.Rule.PrimaryKey

	// Transforming
	//results, err := msg.Rule.Handler.Run(nil, msg.Data.Payload)
	results, err := msg.Rule.Transform(nil, msg.Data.Payload)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		// Ignore
		return nil, nil
	}

	// Fill product_event
	result := results[0]
	fields, err := converter.Convert(msg.Rule.Handler.GetDestinationSchema(), result)
	if err != nil {
		return nil, err
	}

	r := record_type.NewRecord()
	r.Payload.Map.Fields = fields

	// Calcuate primary key
	pk, err := r.CalculateKey(pe.PrimaryKeys)
	if err != nil && err != record_type.ErrNotFoundKeyPath {
		return nil, err
	}

	if pk != nil {
		pe.PrimaryKey = pk
	}

	// Write data back to product event
	pe.SetContent(r)

	//TODO: reuse record object

	return pe, nil
}
