package lte

import "time"

const (
	JSON string = "json"
	GOB         = "gob"
	XML         = "xml"
)

type EventType int32

const (
	CLOSE_IN      EventType = -1
	CLOSE_OUT               = -2
	CLOSE_IN_NOW            = -3
	CLOSE_OUT_NOW           = -4
	STARTUP                 = -5
	PANIC                   = -6
	PANIC_NOW               = -7
	NOT_ALLOWED             = 0
)

type Event interface {
	GetId() int64
	SetId(int64)
	GetTime() time.Time
	SetTime(time.Time)
	GetType() EventType
	SetType(EventType)
}

type BaseEvent struct {
	id    int64
	eTime time.Time
	eType EventType
}

func (e *BaseEvent) GetId() int64 {
	return e.id
}

func (e *BaseEvent) SetId(id int64) {
	e.id = id
}

func (e *BaseEvent) GetTime() time.Time {
	return e.eTime
}

func (e *BaseEvent) SetTime(eTime time.Time) {
	e.eTime = eTime
}

func (e *BaseEvent) GetType() EventType {
	return e.eType
}

func (e *BaseEvent) SetType(eType EventType) {
	e.eType = eType
}
