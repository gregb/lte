package lte

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log"
	"reflect"
	"time"
)

/* Saves long-term events to storage */
type Persister interface {
	/* 	Fetches events which were previously stored.
	Implementors of this interface guarantee that:

	1. All events returned are in chronological order, earliest first.
	2. No event is ever returned twice over any number of calls.
	3. No more than count events are returned.

	*/
	FetchEvents(start time.Time, count int) ([]Event, error)

	/* Stores a future event

	Storing the same event twice is undefined.
	*/
	StoreEvent(e Event) error
}

type EventFactory func(eventType EventType) Event

var SerializationRegistry = make(map[EventType]EventSerializer)
var FactoryRegistry = make(map[EventType]EventFactory)

func RegisterType(eventType EventType, factory EventFactory) {
	bs := &BasicSerializer{eventType: eventType}
	SerializationRegistry[eventType] = bs
	FactoryRegistry[eventType] = factory
	instance := factory(eventType)
	gob.Register(instance)
	t := reflect.TypeOf(instance)
	log.Printf("Registered: %T = %d (%s)", instance, eventType, t.Name())
}

type EventSerializer interface {
	Serialize(string, Event) ([]byte, error)
	Deserialize(string, []byte) (Event, error)
}

type BasicSerializer struct {
	// keep this threadsafe; no state here
	eventType EventType
}

func (s *BasicSerializer) Serialize(method string, e Event) ([]byte, error) {

	var data []byte
	var err error

	switch method {
	case JSON:
		data, err = json.Marshal(e)
	case XML:
		data, err = xml.Marshal(e)
	case GOB:
		data = make([]byte, 0, 100)
		buf := bytes.NewBuffer(data)
		enc := gob.NewEncoder(buf)
		err = enc.Encode(e)
	default:
		err = fmt.Errorf("Unrecognized serialization method <%s>", method)
	}

	if err != nil {
		return nil, err
	}

	return data, nil

}

func (s *BasicSerializer) Deserialize(method string, data []byte) (Event, error) {

	var err error
	factory := FactoryRegistry[s.eventType]
	instance := factory(s.eventType)

	switch method {
	case JSON:
		err = json.Unmarshal(data, instance)
	case XML:
		err = xml.Unmarshal(data, instance)
	case GOB:
		data = make([]byte, 0, 100)
		buf := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buf)
		err = dec.Decode(instance)
	default:
		err = fmt.Errorf("Unrecognized serialization method <%s>", method)
	}

	if err != nil {
		return nil, err
	}

	return instance, nil
}
