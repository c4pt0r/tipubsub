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

package pubsub

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type Store interface {
	Init() error
	CreateStream(streamName string) error
	PutMessages(streamName string, messages []*Message) error
	FetchMessages(streamName string, ts int64, limit int) ([]Message, int64, error)
	MaxID(streamName string) (int64, error)

	MetaPut(key string, value []byte) error
	MetaGet(key string) ([]byte, error)
	MetaDelete(key string) error
	MetaWatch(key string, ch chan<- []byte) error
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
}

func (s *TiDBStore) getStreamTblName(streamName string) string {
	return fmt.Sprintf("tipubsub_stream_%s", streamName)
}

func NewTiDBStore(dsn string) *TiDBStore {
	return &TiDBStore{
		dsn: dsn,
	}
}

func (s *TiDBStore) CreateStream(streamName string) error {
	// TODO: Use partition
	stmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGINT AUTO_INCREMENT,
			ts BIGINT,
			create_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			data TEXT,
			PRIMARY KEY (id),
			KEY(ts)
		);`, s.getStreamTblName(streamName))
	_, err := s.db.Exec(stmt)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(stmt)
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
	return nil
}

func (s *TiDBStore) PutMessages(streamName string, messages []*Message) error {
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
		)`, s.getStreamTblName(streamName))
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

func (s *TiDBStore) FetchMessages(streamName string, idOffset int64, limit int) ([]Message, int64, error) {
	stmt := fmt.Sprintf(`
		SELECT
			id,
			ts,
			data
		FROM %s
		WHERE id > ?
		LIMIT %d`, s.getStreamTblName(streamName), limit)

	rows, err := s.db.Query(stmt, idOffset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var messages []Message
	var maxId int64 = 0
	for rows.Next() {
		var id int64
		var ts int64
		var data []byte
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
	return messages, maxId, nil
}

func (s *TiDBStore) MaxID(streamName string) (int64, error) {
	stmt := fmt.Sprintf(`
		SELECT
			MAX(id)
		FROM %s`, s.getStreamTblName(streamName))
	var maxId int64
	err := s.db.QueryRow(stmt).Scan(&maxId)
	if err != nil {
		return 0, err
	}
	return maxId, nil
}

func (s *TiDBStore) MetaPut(key string, value []byte) error {
	return nil
}

func (s *TiDBStore) MetaGet(key string) ([]byte, error) {
	return nil, nil
}

func (s *TiDBStore) MetaDelete(key string) error {
	return nil
}

func (s *TiDBStore) MetaWatch(key string, ch chan<- []byte) error {
	return nil
}
