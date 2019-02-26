package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockDel struct {
	numOfEventItem  int
	numOfCalls      int
	aggregateResult int
}

func (m *mockDel) delegate(e EventItem, a EventItemAggregate) {
	m.numOfEventItem++

	m.numOfCalls++

	if e.Action == Sum {
		m.aggregateResult = int(a.Value)
	}
}

const testSource EventSource = 10
const testEvent EventName = "test-event"
const oneFloat64 float64 = 1

func TestRealTimeEvents(t *testing.T) {
	es := newEventSink()
	md := mockDel{}

	es.AddSubscription(testSource, RealTime, md.delegate)
	es.AddEvent(testSource, testEvent, "", EventData{Value: 1})
	es.AddEvent(testSource, testEvent, "", EventData{Value: 1})
	es.AddEvent(testSource, testEvent, "", EventData{Value: 1})

	es.FlushAndWait()

	assert.Equal(t, md.numOfEventItem, 3, "Values must match")
	assert.Equal(t, md.numOfCalls, 3, "Values must match")
}

func TestOnDoneEvents(t *testing.T) {
	es := newEventSink()
	md := mockDel{}

	es.AddSubscription(testSource, OnDone, md.delegate)
	es.AddSumEvent(testSource, testEvent, "", oneFloat64)
	es.AddSumEvent(testSource, testEvent, "", oneFloat64)
	es.AddSumEvent(testSource, testEvent, "", oneFloat64)

	assert.Equal(t, md.numOfCalls, 0, "Values must match")

	es.FlushAndWait()

	assert.Equal(t, md.aggregateResult, 3, "Values must match")
	assert.Equal(t, md.numOfCalls, 1, "Values must match")
}

func TestFlushCycle(t *testing.T) {
	es := newEventSink()
	md := mockDel{}

	es.AddSubscription(testSource, RealTime, md.delegate)
	es.AddEvent(testSource, testEvent, "", EventData{Value: 1})

	es.Reset()

	assert.Equal(t, 1, md.numOfEventItem, "Values must match")
	assert.Equal(t, 1, md.numOfCalls, "Values must match")

	es.AddSubscription(testSource, RealTime, md.delegate)
	es.AddEvent(testSource, testEvent, "", EventData{Value: 1})

	es.Reset()

	assert.Equal(t, 2, md.numOfEventItem, "Values must match")
	assert.Equal(t, 2, md.numOfCalls, "Values must match")

	//add a sum event
	es.AddSubscription(testSource, OnDone, md.delegate)
	es.AddSumEvent(testSource, testEvent, "", oneFloat64)

	es.Reset()

	assert.Equal(t, 1, md.aggregateResult, "Values must match")
	assert.Equal(t, 3, md.numOfCalls,  "Values must match")
}
