package pubsub

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type Store interface {
	Init() error
	CreateStream(streamName string) error
	PutMessages(streamName string, messages []Message) error
	FetchMessages(streamName string, ts int64, limit int) ([]Message, int64, error)
	MaxTs(streamName string) (int64, error)

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
			id BIGINT AUTO_RANDOM,
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

func (s *TiDBStore) PutMessages(streamName string, messages []Message) error {
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
		_, err := txn.Exec(sql, msg.Ts, msg.Data)
		if err != nil {
			return err
		}
	}
	err = txn.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (s *TiDBStore) FetchMessages(streamName string, ts int64, limit int) ([]Message, int64, error) {
	stmt := fmt.Sprintf(`
		SELECT
			id,
			ts,
			data
		FROM %s
		WHERE ts > ?
		LIMIT %d`, s.getStreamTblName(streamName), limit)

	rows, err := s.db.Query(stmt, ts)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var messages []Message
	var maxTs int64 = 0
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
		if ts > maxTs {
			maxTs = ts
		}
	}
	return messages, maxTs, nil
}

func (s *TiDBStore) MaxTs(streamName string) (int64, error) {
	stmt := fmt.Sprintf(`
		SELECT
			MAX(ts)
		FROM %s`, s.getStreamTblName(streamName))
	var maxTs int64
	err := s.db.QueryRow(stmt).Scan(&maxTs)
	if err != nil {
		return 0, err
	}
	return maxTs, nil
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
