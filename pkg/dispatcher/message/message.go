package message

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/dispatcher/rule_manager"
	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	"github.com/nats-io/nats.go"
)

type Message struct {
	Msg       *nats.Msg
	Rule      *rule_manager.Rule
	Data      MessageRawData
	Raw       []byte
	Partition int32
	Record    *gravity_sdk_types_record.Record
	RawRecord []byte
}

type MessageRawData struct {
	Event      string `json:"event"`
	RawPayload string `json:"payload"`
	PrimaryKey []byte
	Payload    map[string]interface{}
}

var MessagePool = sync.Pool{
	New: func() interface{} {
		return &Message{}
	},
}

func New() *Message {
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
	err = json.Unmarshal([]byte(m.Data.RawPayload), &m.Data.Payload)
	if err != nil {
		return err
	}

	return nil
}

func (m *Message) Release() {
	MessagePool.Put(m)
}