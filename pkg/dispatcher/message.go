package dispatcher

import (
	"errors"
	"sync"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/rule_manager"
	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	"github.com/BrobridgeOrg/schemer"
	"github.com/nats-io/nats.go"
)

type Message struct {
	ID           string
	Publisher    nats.JetStreamContext
	Msg          *nats.Msg
	Event        string
	Product      *Product
	Rule         *rule_manager.Rule
	Data         MessageRawData
	Raw          []byte
	Partition    int32
	Record       *gravity_sdk_types_record.Record
	RawRecord    []byte
	TargetSchema *schemer.Schema
	OutputMsg    *nats.Msg
	Ignore       bool
}

type MessageRawData struct {
	Event      string `json:"event"`
	RawPayload []byte `json:"payload"`
	//	PrimaryKey []byte
	Payload map[string]interface{}
}

var MessagePool = sync.Pool{
	New: func() interface{} {
		return &Message{}
	},
}

func NewMessage() *Message {
	return MessagePool.Get().(*Message)
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

	if m.Record != nil {
		recordPool.Put(m.Record)
	}

	if m.OutputMsg != nil {
		natsMsgPool.Put(m.OutputMsg)
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
	_, err := m.Publisher.PublishMsgAsync(m.OutputMsg, nats.MsgId(m.ID))
	if err != nil {
		return err
	}

	return nil
}
