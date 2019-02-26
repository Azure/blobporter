package internal

import (
	"fmt"
	"sync"
)

//EventItem represents an event created by either by a Source
type EventItem struct {
	Name     EventName
	Resource string
	Data     []EventData
	Source   EventSource
	Action   EventAction
}

func (e *EventItem) key() string {
	return fmt.Sprintf("%v-%v-%v", e.Source, e.Name, e.Resource)
}

//EventAction used to determined how the event sink will handle the event
type EventAction int

const (
	//Discrete the event flows as distinct unit
	Discrete EventAction = iota
	//Sum the value of the event is aggregated overtime with a sum operation
	Sum
)

//EventName base type for the event name enuk
type EventName string

//EventSource base type for the event source enum
type EventSource int

const (
	//Transfer the event is created by the Transfer instance
	Transfer EventSource = iota
	//Reader the event is created by a Reader
	Reader
	//Worker the event is created by a Worker
	Worker
	//CommitListHandler the event is created by the commit list handler
	CommitListHandler
)

//EventSink singleton instance of the EventSink
var EventSink = newEventSink()

//EventDelegate represents a delegate that handles the event
type EventDelegate func(e EventItem, a EventItemAggregate)

//EventSubscription  represents a subscription for events created by a given Source
type EventSubscription struct {
	Delegate EventDelegate
	Source   EventSource
	Type     EventSubscriptionType
}

//EventSubscriptionType based type of the types of subscriptions
type EventSubscriptionType int

const (
	//RealTime the delegate will be called every time the event is triggered
	RealTime EventSubscriptionType = iota
	//OnDone the delegate will be called only at the end of the transfer
	OnDone
)

//EventItemAggregate represents an event that its value is aggregated overtime
type EventItemAggregate struct {
	NumItems      int64
	LastEventItem EventItem
	Value         float64
}

//EventData key-value tuple with event data
type EventData struct {
	Key   string
	Value interface{}
}

type eventAggregateReq struct {
	key      string
	response chan EventItemAggregate
}

type eventSink struct {
	sync.Mutex
	sums         map[string]EventItemAggregate
	subs         map[EventSource][]EventSubscription
	ondonesubs   map[EventSource][]EventSubscription
	eventsQ      chan EventItem
	wg           *sync.WaitGroup
	workerIsDone bool
}

func newEventSink() *eventSink {
	e := &eventSink{}
	e.init()
	e.startWorker()
	return e

}
func (e *eventSink) Reset() {

	if !e.workerIsDone {
		e.FlushAndWait()
	}

	e.init()
	e.startWorker()

	return
}

func (e *eventSink) startWorker() {
	e.workerIsDone = false
	go func() {
		var sumEvent EventItemAggregate
		defer func() {
			e.workerIsDone = true
		}()
		for {

			event, ok := <-e.eventsQ

			if !ok {

				defer func() {
					e.Lock()
					defer e.Unlock()
					for _, ondonesub := range e.ondonesubs {
						for _, sub := range ondonesub {
							for _, sumEvent := range e.sums {
								if sub.Source == sumEvent.LastEventItem.Source {
									sub.Delegate(sumEvent.LastEventItem, sumEvent)
								}
							}
						}
					}
					e.wg.Done()
				}()

				return
			}

			if event.Action == Sum {
				sumEvent = e.sums[event.key()]
				sumEvent.NumItems++
				sumEvent.LastEventItem = event
				value := event.Data[0].Value.(float64)
				sumEvent.Value += value
				e.sums[event.key()] = sumEvent
			}

			e.Lock()
			esubs := e.subs[event.Source]
			for _, sub := range esubs {
				sub.Delegate(event, sumEvent)
			}
			e.Unlock()
		}
	}()
}
func (e *eventSink) init() {
	e.sums = make(map[string]EventItemAggregate)
	e.subs = make(map[EventSource][]EventSubscription)
	e.ondonesubs = make(map[EventSource][]EventSubscription)
	e.eventsQ = make(chan EventItem, 10000)
	e.wg = &sync.WaitGroup{}
	e.wg.Add(1)
}

//FlushAndWait closese the sink's channels as waits for processing of pending events
func (e *eventSink) FlushAndWait() {
	close(e.eventsQ)
	e.wg.Wait()

}

//AddSubscription adds a subscription to the event sink
func (e *eventSink) AddSubscription(source EventSource, subType EventSubscriptionType, delegate EventDelegate) {
	e.Lock()
	defer e.Unlock()

	sub := EventSubscription{
		Delegate: delegate,
		Source:   source,
		Type:     subType,
	}

	switch subType {
	case RealTime:
		subsForSource := e.subs[source]
		subsForSource = append(subsForSource, sub)
		e.subs[source] = subsForSource
	case OnDone:
		subsForSource := e.ondonesubs[source]
		subsForSource = append(subsForSource, sub)
		e.ondonesubs[source] = subsForSource
	}
}

//AddEvent triggers a discrete event
func (e *eventSink) AddEvent(source EventSource, name EventName, resource string, eventData ...EventData) {
	select {
	case e.eventsQ <- EventItem{
		Source:   source,
		Name:     name,
		Resource: resource,
		Action:   Discrete,
		Data:     eventData,
	}:
	default:
		panic(fmt.Errorf("AddEvent failed the channel is closed or full"))
	}
}

//AddSumEvent  triggers a sum event
func (e *eventSink) AddSumEvent(source EventSource, name EventName, resource string, value float64) {
	select {
	case e.eventsQ <- EventItem{
		Source:   source,
		Name:     name,
		Resource: resource,
		Action:   Sum,
		Data:     []EventData{EventData{Value: value}},
	}:
	default:
		panic(fmt.Errorf("AddSumEvent failed the channel is closed or full"))
	}
}
