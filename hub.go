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
	"errors"
	"sync"
	"time"

	"github.com/c4pt0r/log"
)

var (
	ErrStreamNotFound error = errors.New("stream not found")
)

type Hub struct {
	mu          sync.RWMutex
	pollWorkers map[string]*PollWorker
	store       Store
	// streamName -> Stream
	streams map[string]*Stream
	cfg     *Config

	gcWorker *gcWorker
}

func NewHub(c *Config) (*Hub, error) {
	store, err := OpenStore(c.DSN)
	if err != nil {
		return nil, err
	}
	h := &Hub{
		mu:          sync.RWMutex{},
		cfg:         c,
		store:       store,
		pollWorkers: map[string]*PollWorker{},
		streams:     map[string]*Stream{},
		gcWorker:    newGCWorker(store.DB(), c),
	}
	go h.gc()
	return h, nil
}

func (m *Hub) gc() {
	for {
		time.Sleep(time.Duration(m.cfg.GCIntervalInSec) * time.Second)
		m.mu.RLock()
		for streamName := range m.streams {
			log.I("start GC", streamName)
			m.gcWorker.safeGC(streamName)
		}
		m.mu.RUnlock()
	}
}

func (m *Hub) ForceGC() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for streamName := range m.streams {
		m.gcWorker.safeGC(streamName)
	}
}

func (m *Hub) Publish(streamName string, msg *Message) error {
	m.mu.Lock()
	if _, ok := m.streams[streamName]; !ok {
		stream, err := NewStream(m.cfg, m.store, streamName)
		if err != nil {
			m.mu.Unlock()
			return err
		}
		if err := stream.Open(); err != nil {
			m.mu.Unlock()
			return err
		}
		m.streams[streamName] = stream
	}
	s := m.streams[streamName]
	m.mu.Unlock()
	s.Publish(msg)
	return nil
}

func (m *Hub) MinMaxID(streamName string) (int64, int64, error) {
	return m.store.MinMaxID(streamName)
}

func (m *Hub) PollStat(streamName string) map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if pw, ok := m.pollWorkers[streamName]; ok {
		return pw.Stat()
	}
	return nil
}

func (m *Hub) MessagesSinceOffset(streamName string, offset Offset) ([]Message, error) {
	var ret []Message
	for {
		msgs, newOffsetInt, err := m.store.FetchMessages(streamName, offset, m.cfg.MaxBatchSize)
		if err != nil {
			return nil, err
		}
		if len(msgs) > 0 {
			offset = Offset(newOffsetInt)
			ret = append(ret, msgs...)
		} else {
			break
		}
	}
	return ret, nil
}

func (m *Hub) Subscribe(streamName string, subscriberID string) (<-chan Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// if the stream is not in the map, create a new poll worker for this stream
	if _, ok := m.pollWorkers[streamName]; !ok {
		// create a new poll worker for this stream
		pw, err := newPollWorker(m.cfg, m.store, streamName)
		if err != nil {
			return nil, err
		}
		m.pollWorkers[streamName] = pw
	}
	return m.pollWorkers[streamName].addNewSubscriber(subscriberID)
}

func (m *Hub) Unsubscribe(streamName string, subscriberID string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if pw, ok := m.pollWorkers[streamName]; ok {
		pw.removeSubscriber(subscriberID)
	}
}

func (m *Hub) DB() *sql.DB {
	return m.store.DB()
}
