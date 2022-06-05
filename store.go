// Copyright 2022 Ed Huang<i@huangdx.net>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tipubsub

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Store interface {
	// Init initializes the store, call it after creating the store
	Init() error
	// CreateStream creates a stream
	CreateStream(streamName string) error
	// PutMessages puts messages into a stream
	PutMessages(streamName string, messages []*Message) error
	// FetchMessages fetches messages from a stream
	FetchMessages(streamName string, offset Offset, limit int) ([]Message, Offset, error)
	// MinMaxID returns the min, max offset of a stream
	MinMaxID(streamName string) (int64, int64, error)
	// StartGC starts the garbage collection worker
	GC(streamName string) error
	// DB returns the underlying database
	DB() *sql.DB
}

func OpenStore(dsn string) (Store, error) {
	// TODO only support TiDB now
	s := NewTiDBStore(dsn)
	if err := s.Init(); err != nil {
		return nil, err
	}
	return s, nil
}

type TiDBStore struct {
	dsn string
	db  *sql.DB
	gc  *gcWorker
}

func getStreamTblName(streamName string) string {
	return fmt.Sprintf("tipubsub_stream_%s", streamName)
}

func (s *TiDBStore) getMetaTblName(streamName string) string {
	return fmt.Sprintf("tipubsub_meta_%s", streamName)
}

func NewTiDBStore(dsn string) *TiDBStore {
	return &TiDBStore{
		dsn: dsn,
	}
}

// CreateStream creates a stream, every stream is a table in the database
func (s *TiDBStore) CreateStream(streamName string) error {
	// TODO: Use partition
	// stream is a table in the database
	stmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGINT AUTO_INCREMENT,
			ts BIGINT,
			create_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			data TEXT,
			PRIMARY KEY (id),
			KEY(ts)
		);`, getStreamTblName(streamName))
	_, err := s.db.Exec(stmt)
	if err != nil {
		return err
	}
	return nil
}

func (s *TiDBStore) Init() error {
	var err error
	s.db, err = sql.Open("mysql", s.dsn)
	if err != nil {
		return err
	}

	// default connection timeout setting
	// application could customize it
	// by using the value return of DB()
	s.db.SetConnMaxLifetime(time.Minute * 3)
	s.db.SetMaxOpenConns(50)
	s.db.SetMaxIdleConns(50)

	s.gc = newGCWorker(s.db)
	return nil
}

func (s *TiDBStore) PutMessages(streamName string, messages []*Message) error {
	// a message is a row in the table, so we need to use a transaction
	// because auto_increment is used, we don't need to set id
	// use id as the offset
	txn, err := s.db.Begin()
	defer txn.Rollback()
	if err != nil {
		return err
	}
	for _, msg := range messages {
		sql := fmt.Sprintf(`
		INSERT INTO %s (
			ts,
			data
		) VALUES (
			?,
			?
		)`, getStreamTblName(streamName))
		res, err := txn.Exec(sql, msg.Ts, msg.Data)
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}
		msg.ID = id
	}
	err = txn.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (s *TiDBStore) FetchMessages(streamName string, idOffset Offset, limit int) ([]Message, Offset, error) {
	stmt := fmt.Sprintf(`
		SELECT
			id,
			ts,
			data
		FROM %s
		WHERE id > ?
		LIMIT %d`, getStreamTblName(streamName), limit)

	rows, err := s.db.Query(stmt, idOffset)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	defer rows.Close()

	var messages []Message
	var maxId int64 = 0
	for rows.Next() {
		var id int64
		var ts int64
		var data string
		err := rows.Scan(&id, &ts, &data)
		if err != nil {
			return nil, 0, err
		}
		messages = append(messages, Message{
			ID:   id,
			Ts:   ts,
			Data: data,
		})
		if id > maxId {
			maxId = id
		}
	}
	return messages, Offset(maxId), nil
}

func (s *TiDBStore) MinMaxID(streamName string) (int64, int64, error) {
	// using isnull make sure when there is no message in the stream, not return NULL
	stmt := fmt.Sprintf(`
		SELECT
			IFNULL(MIN(id), 0),
			IFNULL(MAX(id), 0)
		FROM %s`, getStreamTblName(streamName))
	var minId, maxId int64
	err := s.db.QueryRow(stmt).Scan(&minId, &maxId)
	if err != nil {
		return -1, -1, err
	}
	return minId, maxId, nil
}

func (s *TiDBStore) GC(streamName string) error {
	return s.gc.safeGC(streamName)
}

func (s *TiDBStore) DB() *sql.DB {
	return s.db
}
