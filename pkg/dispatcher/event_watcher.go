package dispatcher

import (
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/BrobridgeOrg/gravity-sdk/v2/core"
	"github.com/nats-io/nats.go"
)

const (
	domainStream       = "GVT_%s"
	domainEventSubject = "$GVT.%s.EVENT.%s"
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
	running bool
}

func NewEventWatcher(client *core.Client, domain string, durable string) *EventWatcher {
	return &EventWatcher{
		client:  client,
		domain:  domain,
		durable: durable,
		events:  make(map[string]*Event),
		running: false,
	}
}

func (ew *EventWatcher) RegisterEvent(name string) *Event {

	if e, ok := ew.events[name]; ok {
		return e
	}

	e := NewEvent()
	e.Name = name

	subject := fmt.Sprintf(domainEventSubject, ew.domain, name)

	logger.Info("Registered event",
		zap.String("subject", subject),
	)

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

	// Preparing JetStream
	js, err := ew.client.GetJetStream()
	if err != nil {
		return err
	}

	streamName := fmt.Sprintf(domainStream, ew.domain)

	logger.Info("Initializing event stream",
		zap.String("stream", streamName),
	)

	// Check if the stream already exists
	stream, err := js.StreamInfo(streamName)
	if err != nil {
		if err != nats.ErrStreamNotFound {
			logger.Error("Failed to get stream information",
				zap.Error(err),
			)
			return err
		}

		logger.Warn("event stream not found",
			zap.String("stream", streamName),
		)
	}

	subject := fmt.Sprintf(domainEventSubject, ew.domain, "*")

	if stream == nil {

		// Initializing stream
		logger.Info("Creating stream...",
			zap.String("stream", streamName),
			zap.String("subject", subject),
		)

		// Attempt to set three replicas
		sConfig := &nats.StreamConfig{
			Name:        streamName,
			Description: "Gravity domain event store",
			Duplicates:  30 * time.Minute,
			Subjects: []string{
				subject,
			},
			Replicas: 3,
			//			Retention: nats.InterestPolicy,
		}

		_, err := js.AddStream(sConfig)
		if err != nil {

			// Attempt to set one replicas for single node
			sConfig.Replicas = 1
			_, err := js.AddStream(sConfig)
			if err != nil {
				return err
			}
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

func (ew *EventWatcher) AssertConsumer() (*nats.ConsumerInfo, error) {

	// Preparing JetStream
	js, err := ew.client.GetJetStream()
	if err != nil {
		return nil, err
	}

	streamName := fmt.Sprintf(domainStream, ew.domain)

	logger.Info("Checking consumer",
		zap.String("stream", streamName),
		zap.String("consumer", ew.durable),
	)

	c, err := js.ConsumerInfo(streamName, ew.durable)
	if err != nil {
		if err != nats.ErrConsumerNotFound {
			return nil, err
		}

		subject := fmt.Sprintf(domainEventSubject, ew.domain, ">")

		logger.Info("Creating a new consumer...",
			zap.String("stream", streamName),
			zap.String("subject", subject),
		)

		cfg := nats.ConsumerConfig{
			Durable: ew.durable,
			//			DeliverSubject: nats.NewInbox(),
			FilterSubject: subject,
			AckPolicy:     nats.AckAllPolicy,
			//			MaxAckPending: 2048,
		}

		c, err := js.AddConsumer(streamName, &cfg)
		if err != nil {
			return c, err
		}

		return c, nil
	}

	logger.Info("Consumer exists already",
		zap.String("stream", streamName),
		zap.String("consumer", ew.durable),
	)

	return c, nil
}

func (ew *EventWatcher) subscribe(subject string, fn func(string, *nats.Msg)) error {

	// Preparing JetStream
	js, err := ew.client.GetJetStream()
	if err != nil {
		return err
	}

	sub, err := js.PullSubscribe(subject, ew.durable)
	if err != nil {
		return err
	}

	sub.SetPendingLimits(2048, -1)
	ew.client.GetConnection().Flush()

	ew.sub = sub
	ew.running = true

	go func() {

		logger.Info("Waiting events...",
			zap.String("subject", subject),
			zap.String("durable", ew.durable),
		)

		for ew.running {

			msgs, err := sub.Fetch(512, nats.MaxWait(time.Second))
			if err != nil {

				if err == nats.ErrTimeout {
					continue
				}

				logger.Error(err.Error())
			}

			logger.Info("received messages",
				zap.String("subject", subject),
				zap.String("durable", ew.durable),
				zap.Int("count", len(msgs)),
			)

			for _, msg := range msgs {

				// Ignore event
				e, ok := ew.events[msg.Subject]
				if !ok {
					fn("", msg)
					continue
				}

				fn(e.Name, msg)
			}
		}
	}()

	return nil
}

func (ew *EventWatcher) Watch(fn func(string, *nats.Msg)) error {

	// Watching already
	if ew.sub != nil {
		return nil
	}

	logger.Info("Start watching for events...")
	/*
		// Preparing JetStream
		js, err := ew.client.GetJetStream()
		if err != nil {
			return err
		}
	*/
	// Initializing consumer
	_, err := ew.AssertConsumer()
	if err != nil {
		return err
	}

	subject := fmt.Sprintf(domainEventSubject, ew.domain, ">")

	err = ew.subscribe(subject, fn)
	if err != nil {
		logger.Error(err.Error())
		return err
	}
	/*
		//sub, err := js.PullSubscribe(subject, "DISPATCH", nats.PullMaxWaiting(128), nats.AckExplicit())

		sub, err := js.Subscribe(subject, func(msg *nats.Msg) {
			logger.Info("Received event",
				zap.String("subject", msg.Subject),
			)

			// Ignore event
			e, ok := ew.events[msg.Subject]
			if !ok {
				fn("", msg)
				return
			}

			fn(e.Name, msg)

			//}, nats.DeliverNew(), nats.AckAll(), nats.Durable(ew.durable), nats.OrderedConsumer())
			//}, nats.OrderedConsumer())
			//}, nats.MaxAckPending(20480), nats.AckAll(), nats.Durable(ew.durable))
		}, nats.Durable(ew.durable))
		if err != nil {
			logger.Error(err.Error())
			return err
		}
	*/

	return nil
}

func (ew *EventWatcher) Stop() error {

	if !ew.running {
		return nil
	}

	ew.running = false

	if ew.sub == nil {
		return nil
	}

	sub := ew.sub
	ew.sub = nil

	return sub.Unsubscribe()
}
