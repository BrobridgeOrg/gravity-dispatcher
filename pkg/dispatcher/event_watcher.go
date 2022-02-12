package dispatcher

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/BrobridgeOrg/gravity-sdk/core"
	"github.com/nats-io/nats.go"
)

type WatcherManager struct {
	watchers map[string]*EventWatcher
}

func NewWatcherManager() *WatcherManager {
	return &WatcherManager{
		watchers: make(map[string]*EventWatcher),
	}
}

func (wm *WatcherManager) Get(name string) *EventWatcher {
	if v, ok := wm.watchers[name]; ok {
		return v
	}

	return nil
}

func (wm *WatcherManager) Delete(name string) {
	delete(wm.watchers, name)
}

type Event struct {
	Name string
}

func NewEvent() *Event {
	return &Event{}
}

type EventWatcher struct {
	client  *core.Client
	domain  string
	durable string
	events  map[string]*Event
	sub     *nats.Subscription
}

func NewEventWatcher(client *core.Client, domain string, durable string) *EventWatcher {
	return &EventWatcher{
		client:  client,
		domain:  domain,
		durable: durable,
		events:  make(map[string]*Event),
	}
}

func (ew *EventWatcher) RegisterEvent(name string) *Event {

	if e, ok := ew.events[name]; ok {
		return e
	}

	e := NewEvent()
	e.Name = name

	subject := fmt.Sprintf("GRAVITY-%s.EVENT.%s", ew.domain, name)

	ew.events[subject] = e

	return e
}

func (ew *EventWatcher) UnregisterEvent(name string) {

	if _, ok := ew.events[name]; !ok {
		return
	}

	delete(ew.events, name)
}

func (ew *EventWatcher) PurgeEvent() {
	ew.events = make(map[string]*Event)
}

func (ew *EventWatcher) GetEvent(name string) *Event {

	if v, ok := ew.events[name]; ok {
		return v
	}

	return nil
}

func (ew *EventWatcher) Init() error {

	logger.Info("Initializing EventWatcher...")

	// Preparing JetStream
	js, err := ew.client.GetJetStream()
	if err != nil {
		return err
	}

	streamName := fmt.Sprintf("GRAVITY-%s", ew.domain)

	// Check if the stream already exists
	stream, err := js.StreamInfo(streamName)
	if err != nil {
		logger.Warn(err.Error())
	}

	subject := fmt.Sprintf("%s.EVENT.*", streamName)

	if stream == nil {

		// Initializing stream
		logger.Info("Creating stream...",
			zap.String("stream", streamName),
			zap.String("subject", subject),
		)

		_, err := js.AddStream(&nats.StreamConfig{
			Name:        streamName,
			Description: "Gravity event store",
			Subjects: []string{
				subject,
			},
			Retention: nats.InterestPolicy,
		})

		if err != nil {
			return err
		}
	}
	/*
		// Initializing consumer
		_, err = js.AddConsumer(streamName, &nats.ConsumerConfig{
			Durable: "DISPATCH",
			//		AckPolicy: nats.AckAllPolicy,
		})
		if err != nil {
			return err
		}
	*/
	return nil
}

func (ew *EventWatcher) Watch(fn func(string, *nats.Msg)) error {

	// Watching already
	if ew.sub != nil {
		return nil
	}

	logger.Info("Starting watch for events...")

	// Preparing JetStream
	js, err := ew.client.GetJetStream()
	if err != nil {
		return err
	}

	streamName := fmt.Sprintf("GRAVITY-%s", ew.domain)

	subject := fmt.Sprintf("%s.EVENT.*", streamName)
	logger.Info("Waiting events...",
		zap.String("subject", subject),
	)
	//sub, err := js.PullSubscribe(subject, "DISPATCH", nats.PullMaxWaiting(128), nats.AckExplicit())
	sub, err := js.Subscribe(subject, func(msg *nats.Msg) {

		// Ignore event
		if e, ok := ew.events[msg.Subject]; ok {
			fn(e.Name, msg)
			return
		}

		msg.Ack()
	}, nats.DeliverNew(), nats.AckAll(), nats.Durable(ew.durable))
	if err != nil {
		return err
	}

	ew.sub = sub

	return nil
}

func (ew *EventWatcher) Stop() error {

	if ew.sub == nil {
		return nil
	}

	return ew.sub.Unsubscribe()
}
