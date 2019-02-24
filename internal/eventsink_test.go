package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockDel struct {
	numOfEventItem          int
	numOfEventItemAggregate int
	numOfCalls              int
}

func (m *mockDel) delegate(e EventItem, a EventItemAggregate) {
	m.numOfEventItem++
	m.numOfEventItemAggregate++
	m.numOfCalls++
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

	assert.Equal(t, md.numOfEventItemAggregate, 3, "Values must match")
	assert.Equal(t, md.numOfCalls, 3, "Values must match")
}

func TestFlushCycle(t *testing.T) {
	es := newEventSink()
	md := mockDel{}

	es.AddSubscription(testSource, RealTime, md.delegate)
	es.AddEvent(testSource, testEvent, "", EventData{Value: 1})

	assert.Equal(t, md.numOfEventItem, 1, "Values must match")
	assert.Equal(t, md.numOfCalls, 1, "Values must match")

	err := es.Reset()
	assert.Error(t, err, "Reset must fail as it hasn't been flushed")

	//even if the reset fail the sink must be able to receive events
	es.AddEvent(testSource, testEvent, "", EventData{Value: 1})
	assert.Equal(t, md.numOfEventItem, 2, "Values must match")
	assert.Equal(t, md.numOfCalls, 2, "Values must match")

	//add a sum event
	es.AddSubscription(testSource, OnDone, md.delegate)
	es.AddSumEvent(testSource, testEvent, "", oneFloat64)

	//the sum event must be triggered at the end...
	assert.Equal(t, md.numOfCalls, 2, "Values must match")

	es.FlushAndWait()

	assert.Equal(t, md.numOfEventItemAggregate, 1, "Values must match")
	assert.Equal(t, md.numOfCalls, 3, "Values must match")

	err = es.Reset()
	assert.NoError(t, err, "Reset must succeed as the sink is flushed")

}
