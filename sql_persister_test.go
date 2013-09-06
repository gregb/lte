package lte

import (
	"database/sql"
	"fmt"
	_ "github.com/gregb/pq"
	"launchpad.net/gocheck"
	"math/rand"
	"os"
	"testing"
	"time"
)

const q_create_table = `
DROP TABLE IF EXISTS event;

CREATE TABLE event
(
  id bigserial NOT NULL,
  "time" timestamp with time zone NOT NULL,
  type integer NOT NULL,
  method character varying NOT NULL,
  data character varying NOT NULL,
  retrieved timestamp with time zone,
  CONSTRAINT event_pkey PRIMARY KEY (id)
);

CREATE INDEX event_time_idx
  ON event
  USING btree
  ("time")
  WHERE retrieved IS NULL;`

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { gocheck.TestingT(t) }

type Suite struct {
	db *sql.DB
}

type specificEvent struct {
	BaseEvent
	Foo string
	Bar int64
}

func (s *specificEvent) Type() EventType {
	return 1
}

func NewSpecificEvent() *specificEvent {

	e := new(specificEvent)
	return e
}

var _ = gocheck.Suite(&Suite{})

type Fatalistic interface {
	Fatal(args ...interface{})
}

func (s *Suite) SetUpSuite(c *gocheck.C) {
	datname := os.Getenv("PGDATABASE")
	sslmode := os.Getenv("PGSSLMODE")

	if datname == "" {
		os.Setenv("PGDATABASE", "pqgotest")
	}

	if sslmode == "" {
		os.Setenv("PGSSLMODE", "disable")
	}

	var err error
	s.db, err = sql.Open("postgres", "user=pqgotest dbname=pqgotest password=pqgotest sslmode=disable")

	if err != nil {
		c.Fatal(err)
	}

	_, err = s.db.Exec(q_create_table)

	if err != nil {
		c.Fatal(err)
	}

	RegisterType(1, func(eventType EventType) Event {
		return new(specificEvent)
	})
}

func (s *Suite) TestManual(c *gocheck.C) {

	c.Skip("-")

	p := NewSqlPersister(s.db, JSON, "event")
	m := NewManager(p)

	m.Run()

	t0 := time.Now()

	e1 := new(specificEvent)
	e1.eType = 1
	e1.eTime = t0.Add(time.Second * 10)
	e1.Foo = "event 1"
	e1.Bar = 10

	e2 := new(specificEvent)
	e2.eType = 1
	e2.eTime = t0.Add(time.Second * 30)
	e2.Foo = "event 2"
	e2.Bar = 30

	e3 := new(specificEvent)
	e3.eType = 1
	e3.eTime = t0.Add(time.Second * 50)
	e3.Foo = "event 3"
	e3.Bar = 50

	e4 := new(specificEvent)
	e4.eType = 1
	e4.eTime = t0.Add(time.Second * 80)
	e4.Foo = "event 4"
	e4.Bar = 80

	m.In() <- e1
	m.In() <- e2
	m.In() <- e3
	m.In() <- e4

	err := m.Wait(time.Second * 120)

	if err != nil {
		c.Errorf("Error running manager: %s", err)
	}
}

func (s *Suite) TestLong(c *gocheck.C) {

	// something higher than the shortTermCapacity...
	const count = 1000
	const spacing time.Duration = time.Second / 20

	p := NewSqlPersister(s.db, JSON, "event")
	m := NewManager(p)

	m.Run()

	events := make([]*specificEvent, 0, count)

	t0 := time.Now().Add(5 * time.Second)

	fireTime := rand.Perm(count)

	for i := 0; i < count; i++ {
		e := new(specificEvent)
		e.eType = 1
		dt := time.Duration(fireTime[i]) * spacing
		e.eTime = t0.Add(dt)
		e.Bar = int64(fireTime[i])
		e.Foo = fmt.Sprintf("Event %d: t0 + %f ms, @%v", i, float64(dt)/float64(time.Millisecond), e.eTime)

		events = append(events, e)
		m.In() <- e
	}

	m.Wait(count*spacing + 10)

}
