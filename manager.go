package lte

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type Manager interface {
	Run()
	Wait(time.Duration) error
	In() chan<- Event
	Out() <-chan Event
}

type manager struct {
	shortTermCapacity int
	shortTerm         time.Time
	shortTermLock     sync.Mutex
	persister         Persister
	in                chan Event
	out               chan Event
	done              chan bool
}

func NewManager(p Persister) Manager {
	m := new(manager)
	m.shortTerm = time.Now().Add(30 * time.Second)
	m.shortTermCapacity = 100
	m.in = make(chan Event, 100)
	m.out = make(chan Event, 100)
	m.done = make(chan bool)
	m.persister = p
	return m
}

func (m *manager) primeEvent(e Event) error {

	duration := e.GetTime().Sub(time.Now())

	log.Printf("Primer: Priming event: %v, firing in %d ms", e, duration/time.Millisecond)

	// create a timer that executes func that broadcasts e after duration
	time.AfterFunc(duration, func() {
		log.Printf("Timer: Firing event: %v", e)

		if e.GetType() < 0 {
			log.Printf("Timer: Control event %d", e.GetType())
			switch e.GetType() {
			case CLOSE_OUT:
				close(m.out)
				m.done <- true
			case CLOSE_IN:
				close(m.in)
			case PANIC:
				log.Panicf("Timer: Received PANIC event type: %v", e)
			default:
				m.out <- e
			}
		} else {
			m.out <- e
		}

	})

	return nil
}

func (m *manager) processInboundEvents() error {

	log.Printf("Inbound: Now accepting events at %v", time.Now())
	log.Printf("Inbound: Initial shortTerm = %s", m.shortTerm)

	defer func() {
		if r := recover(); r != nil {
			log.Panicf("Inbound: Panic: %s", r)
		}
	}()

	for e := range m.in {
		log.Printf("Inbound: New event on in channel: %v", e)

		if e.GetType() == 0 {
			log.Printf("Inbound: EventType 0 not allowed; event rejected: %v", e)
			continue
		}

		// handle control events
		if e.GetType() < 0 {
			switch e.GetType() {
			case CLOSE_OUT_NOW:
				close(m.out)
				m.done <- true
			case CLOSE_IN_NOW:
				close(m.in)
			case PANIC:
				log.Panicf("Inbound: Received PANIC event type: %v", e)
			}
			continue
		}

		// all valid events get stored
		err := m.persister.StoreEvent(e)
		if err != nil {
			log.Printf("Inbound: Error persisting event: %v\n\t%s", e, err)
			return err
		}

		// events with no time are immediate
		if e.GetTime().IsZero() {
			log.Printf("Inbound: Immediate event , processing now: %v", e)
			m.out <- e
			continue
		}

		// if event is closer than shortTerm, or we have no persister, prime it now
		if e.GetTime().Before(m.shortTerm) || m.persister == nil {
			// if persistance watcher is in the process of calculating a new shortTerm, then wait for it
			m.shortTermLock.Lock()
			log.Printf("Inbound: Event time (%s) is before shortTerm %s", e.GetTime(), m.shortTerm)

			err := m.primeEvent(e)
			if err != nil {
				return err
			}
			m.shortTermLock.Unlock()
		}
		log.Printf("Inbound: Waiting for new event to arrive")
	}

	log.Println("Inbound: No longer accepting events")
	close(m.in)
	return nil
}

func (m *manager) processLongtermEvents() error {

	log.Println("Longterm: Processing persisted events...")

	for {
		// fetch next N events up for dispatch
		log.Printf("Longterm: Checking persister for events...")
		m.shortTermLock.Lock()
		events, err := m.persister.FetchEvents(m.shortTerm, m.shortTermCapacity)

		if err != nil {
			return err
		}

		if len(events) > 0 {
			log.Printf("Longterm: Found %d new events to prime", len(events))
			// prime all the events
			for _, e := range events {

				err := m.primeEvent(e)
				if err != nil {
					return err
				}

				// update shortTerm as we go
				if e.GetTime().After(m.shortTerm) {
					m.shortTerm = e.GetTime()
				}
			}
		} else {
			log.Printf("Longterm: No persisted events")
			m.shortTerm = time.Now().Add(30 * time.Second)
		}
		m.shortTermLock.Unlock()

		log.Printf("Longterm: New shortTerm = %s", m.shortTerm)

		// go back to sleep until 30 seconds before the new shortTerm
		duration := m.shortTerm.Sub(time.Now())
		timer := time.NewTimer(duration)

		log.Printf("Longterm: Sleeping for %d ms ...", duration/time.Millisecond)
		<-timer.C
		log.Println("Longterm: Back awake")

		// we're back awake... do it all again
	}

	return nil
}

func (m *manager) Run() {

	if m.persister != nil {
		go m.processLongtermEvents()
	} else {
		log.Println("Manager: Warning - running without persister; all events will be primed immediately")
	}

	go m.processInboundEvents()

	// record the startup
	startup := &BaseEvent{eType: STARTUP}
	m.in <- startup
}

func (m *manager) In() chan<- Event {
	return m.in
}

func (m *manager) Out() <-chan Event {
	return m.out
}

// Blocks until the manager receives a CLOSE_OUT event, or timeout expires
// If timeout expires, an error is returned
func (m *manager) Wait(timeout time.Duration) error {

	if timeout > 0 {
		time.AfterFunc(timeout, func() {
			log.Println("Manager: Wait timeout expired")
			m.done <- false
		})
	}

	timeoutExpired := <-m.done

	if timeoutExpired {
		return fmt.Errorf("Manager: Wait over via timeout after %d ms", timeout/time.Millisecond)
	}

	return nil
}
