package dispatcher

import (
	"errors"
	"sync"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/rule_manager"
	gravity_sdk_types_product_event "github.com/BrobridgeOrg/gravity-sdk/v2/types/product_event"
	"github.com/BrobridgeOrg/schemer"

	"github.com/nats-io/nats.go"
)

type Message struct {
	ID              string
	Publisher       nats.JetStreamContext
	Msg             *nats.Msg
	AckFuture       nats.PubAckFuture
	Event           string
	Product         *Product
	Rule            *rule_manager.Rule
	Data            *MessageRawData
	Raw             []byte
	Partition       int32
	ProductEvent    *gravity_sdk_types_product_event.ProductEvent
	RawProductEvent []byte
	TargetSchema    *schemer.Schema
	OutputMsg       *nats.Msg
	Ignore          bool
}

type MessageRawData struct {
	Event      string `json:"event"`
	RawPayload []byte `json:"payload"`
	//	PrimaryKey []byte
	Payload map[string]interface{}
}

var MessagePool = sync.Pool{
	New: func() interface{} {
		return &Message{
			Data: &MessageRawData{},
		}
	},
}

func NewMessage() *Message {
	m := MessagePool.Get().(*Message)
	m.Reset()
	return m
}

func (m *Message) ParseRawData() error {

	// Parsing raw data
	err := json.Unmarshal(m.Raw, &m.Data)
	if err != nil {
		return err
	}

	if len(m.Data.RawPayload) == 0 {
		return errors.New("Empty payload")
	}

	// Parsing payload
	err = json.Unmarshal(m.Data.RawPayload, &m.Data.Payload)
	if err != nil {
		return err
	}

	return nil
}

func (m *Message) Release() {
	m.Reset()
	MessagePool.Put(m)
}

func (m *Message) Reset() {

	if m.ProductEvent != nil {
		productEventPool.Put(m.ProductEvent)
		m.ProductEvent = nil
	}

	if m.OutputMsg != nil {
		natsMsgPool.Put(m.OutputMsg)
		m.OutputMsg = nil
	}

	m.Msg = nil
	m.AckFuture = nil
	m.Rule = nil
	m.Product = nil
	m.ProductEvent = nil
	m.OutputMsg = nil
	m.TargetSchema = nil
	m.Event = ""
	m.Raw = []byte("")
	m.RawProductEvent = []byte("")
	m.Ignore = false
	m.Data = &MessageRawData{
		Payload: make(map[string]interface{}),
	}
}

func (m *Message) Ack() error {
	return m.Msg.Ack()
}

func (m *Message) Dispatch() error {

	if m.Ignore {
		return nil
	}

	if m.Publisher == nil {
		return nil
	}

	if !m.Product.Enabled {
		return nil
	}

	// Publish to product stream
	future, err := m.Publisher.PublishMsgAsync(m.OutputMsg, nats.MsgId(m.ID))
	if err != nil {
		return err
	}

	m.AckFuture = future

	return nil
}

func (m *Message) Wait() error {

	if m.AckFuture == nil {
		return nil
	}

	select {
	case <-m.AckFuture.Ok():
	case err := <-m.AckFuture.Err():
		return err
	}

	return nil
}
