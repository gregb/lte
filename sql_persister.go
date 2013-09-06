package lte

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

const q_get_events = `select id, type, time, method, data from event where time >= $1 and retrieved is null order by time asc limit $2`
const q_update_fetched = `update event set retrieved = $1 where id = any($2)`
const q_insert_event = `insert into event("time", type, method, data) values ($1, $2, $3, $4) returning id`

type sqlPersister struct {
	db     *sql.DB
	get    *sql.Stmt
	update *sql.Stmt
	insert *sql.Stmt
	method string
}

func NewSqlPersister(db *sql.DB, method string, tableName string) *sqlPersister {
	p := new(sqlPersister)
	p.db = db
	p.method = method

	// TODO: Make table name a replaceable token

	var err error

	p.get, err = db.Prepare(q_get_events)
	if err != nil {
		log.Panicf("Persister: Error preparing query: %s", err)
	}

	p.update, err = db.Prepare(q_update_fetched)
	if err != nil {
		log.Panicf("Persister: Error preparing query: %s", err)
	}

	p.insert, err = db.Prepare(q_insert_event)
	if err != nil {
		log.Panicf("Persister: Error preparing query: %s", err)
	}

	return p
}

func (p *sqlPersister) FetchEvents(start time.Time, count int) ([]Event, error) {

	log.Printf("Persister: Fetching %d events starting at %s", count, start)
	tx, err := p.db.Begin()

	if err != nil {
		log.Panicf("Persister: Error beginning transaction: %s", err)
	}

	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			log.Panicf("Persister: Error during transaction, rolling back: %s", err)
		} else {
			err = tx.Commit()
			if err != nil {
				log.Panicf("Persister: Error committing transaction, rolling back: %s", err)
				tx.Rollback()
			}
		}
	}()

	rows, err := tx.Stmt(p.get).Query(start, count)

	if err != nil {
		log.Panicf("Persister: Error fetching events: %s", err)
	}

	var id int64
	var eType EventType
	var eTime time.Time
	var method string
	var data []byte

	events := make([]Event, 0, count)
	ids := make([]int64, 0, count)

	// get the events
	for rows.Next() {

		rows.Scan(&id, &eType, &eTime, &method, &data)

		serializer, _ := SerializationRegistry[eType]
		e, err := serializer.Deserialize(method, data)

		// fill in other fields
		e.SetId(id)
		e.SetType(eType)
		e.SetTime(eTime)

		if err != nil {
			log.Panicf("Persister: Error deserializing event: %s", err)
		}

		log.Printf("Persister: Found event: %v", e)

		events = append(events, e)
		ids = append(ids, id)
	}

	rows.Close()

	if len(ids) > 0 {
		// mark them as retrieved
		log.Printf("Persister: Marking ids as retrieved: %v", ids)
		result, err := tx.Stmt(p.update).Exec(time.Now(), ids)

		if err != nil {
			log.Panicf("Persister: Error updating fetched events: %s", err)
		}

		updateCount, err := result.RowsAffected()

		if err != nil {
			log.Panicf("Persister: Error determining the number of rows updated: %s", err)
		}

		if updateCount != int64(len(ids)) {
			log.Panicf("Persister: Tried to mark %d events as fetched, but only %d rows updated", len(ids), updateCount)
		}
	}

	return events, nil
}

func (p *sqlPersister) StoreEvent(e Event) error {

	serializer, ok := SerializationRegistry[e.GetType()]

	if !ok {
		log.Panicf("Persister: Prefer method <%s> to store event, but no serializer registered to handle it.", p.method)
	}

	data, err := serializer.Serialize(p.method, e)

	if err != nil {
		log.Panicf("Persister: Error serializing event: %s", err)
	}

	scheduled := e.GetTime()
	if scheduled.IsZero() {
		scheduled = time.Now()
	}

	results, err := p.insert.Exec(e.GetTime(), e.GetType(), p.method, data)

	if err != nil {
		log.Panicf("Persister: Error inserting event: %s", err)
	}

	insertedRows, err := results.RowsAffected()

	if err != nil {
		log.Panicf("Persister: Error determining number of rows inserted: %s", err)
	}

	if insertedRows != 1 {
		log.Panicf("Persister: Expected 1 row inserted, but db reports %d", insertedRows)
	}

	id, err := results.LastInsertId()

	if err != nil {
		// not a data-loss error, so don't need to panic, i think
		return fmt.Errorf("Persister: Error getting id of row inserted: %s", err)
	}

	e.SetId(id)

	return nil
}
