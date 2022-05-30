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
	"sync/atomic"
	"time"

	"github.com/c4pt0r/log"
)

var (
	ErrStreamNotFound error = errors.New("stream not found")
)

// PollWorker is a worker that polls messages from a stream
type PollWorker struct {
	cfg              *Config
	streamName       string
	lastSeenOffset   int64
	maxBatchSize     int
	store            Store
	pollIntervalInMs int
	stopped          atomic.Value
	numSubscribers   int32

	// make sure subscribers is threadsafe here
	mu sync.Mutex
	// subscribers map[string]Subscriber, key is subscriber id
	subscribers map[string]Subscriber
}

func newPollWorker(cfg *Config, s Store, streamName string, offset int64) (*PollWorker, error) {
	// create stream table
	err := s.CreateStream(streamName)
	if err != nil {
		return nil, err
	}

	if offset == LatestId {
		_, offset, err = s.MinMaxID(streamName)
		if err != nil {
			return nil, err
		}
	}
	stopped := atomic.Value{}
	stopped.Store(false)

	pw := &PollWorker{
		streamName:       streamName,
		lastSeenOffset:   offset,
		maxBatchSize:     cfg.MaxBatchSize,
		store:            s,
		stopped:          stopped,
		numSubscribers:   0,
		mu:               sync.Mutex{},
		pollIntervalInMs: cfg.PollIntervalInMs,
		subscribers:      map[string]Subscriber{},
	}
	go pw.run()
	return pw, nil
}

func (pw *PollWorker) addNewSubscriber(subscriber Subscriber, offset int64) error {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	log.I("pollWorkers", pw.streamName, "got new subscriber:", subscriber.ID())
	// push [offset, max] to new subscriber
	if offset != LatestId && offset < pw.lastSeenOffset {
		for {
			msgs, newOffset, err := pw.store.FetchMessages(pw.streamName, offset, pw.maxBatchSize)
			if err != nil {
				return err
			}
			if len(msgs) > 0 {
				offset = newOffset
				go subscriber.OnMessages(pw.streamName, msgs)
			} else {
				break
			}
		}
	}
	// listen to new messages
	pw.subscribers[subscriber.ID()] = subscriber
	atomic.AddInt32(&pw.numSubscribers, 1)
	return nil
}

func (pw *PollWorker) removeSubscriber(subscriber Subscriber) {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	log.I("pollWorkers", pw.streamName, "remove subscriber:", subscriber.ID())
	delete(pw.subscribers, subscriber.ID())
	atomic.AddInt32(&pw.numSubscribers, -1)
}

func (pw *PollWorker) Stop() {
	log.I("pollWorkers", pw.streamName, "stopped")
	pw.stopped.Store(true)
}

func (pw *PollWorker) run() {
	log.Info("sub: start polling from", pw.streamName, "@id=", pw.lastSeenOffset)
	for !pw.stopped.Load().(bool) {
		// get messages from the stream in batches
		msgs, max, err := pw.store.FetchMessages(pw.streamName, pw.lastSeenOffset, pw.maxBatchSize)
		if err != nil {
			log.Error(err)
			time.Sleep(time.Duration(pw.pollIntervalInMs) * time.Millisecond)
			goto done
		}
		if len(msgs) > 0 {
			pw.lastSeenOffset = max
			log.Info("sub: got", len(msgs), "messages from", pw.streamName, "@ id=", pw.lastSeenOffset)

			pw.mu.Lock()
			// fanout to subscribers
			for _, value := range pw.subscribers {
				subscriber := value.(Subscriber)
				log.Info("fanout to", subscriber.ID())
				go subscriber.OnMessages(pw.streamName, msgs)
			}
			pw.mu.Unlock()
		}
	done:
		time.Sleep(time.Duration(pw.pollIntervalInMs) * time.Millisecond)
	}
	log.D("poll worker stopped")
}

type Hub struct {
	mu          sync.RWMutex
	pollWorkers map[string]*PollWorker
	store       Store
	// streamName -> Stream
	streams map[string]*Stream
	cfg     *Config
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
			m.store.GC(streamName)
		}
		m.mu.RUnlock()
	}
}

func (m *Hub) ForceGC() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for streamName := range m.streams {
		m.store.GC(streamName)
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

func (m *Hub) Subscribe(streamName string, subscriber Subscriber, offsetID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// if the stream is not in the map, create a new poll worker for this stream
	if _, ok := m.pollWorkers[streamName]; !ok {
		// create a new poll worker for this stream
		pw, err := newPollWorker(m.cfg, m.store, streamName, offsetID)
		if err != nil {
			return err
		}
		m.pollWorkers[streamName] = pw
	}
	return m.pollWorkers[streamName].addNewSubscriber(subscriber, offsetID)
}

func (m *Hub) Unsubscribe(streamName string, subscriber Subscriber) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if pw, ok := m.pollWorkers[streamName]; ok {
		pw.removeSubscriber(subscriber)
	}
}

func (m *Hub) DB() *sql.DB {
	return m.store.DB()
}
