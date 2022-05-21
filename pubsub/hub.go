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
	"sync"
	"sync/atomic"
	"time"

	"github.com/c4pt0r/log"
)

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
	mu          sync.Mutex
	subscribers map[string]Subscriber
}

func newPollWorker(cfg *Config, s Store, streamName string, offset int64) (*PollWorker, error) {
	// create stream table
	err := s.CreateStream(streamName)
	if err != nil {
		return nil, err
	}

	if offset == LatestId {
		offset, err = s.MaxID(streamName)
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
		pollIntervalInMs: cfg.PollIntervalInMs, // FIXME: use config
		subscribers:      map[string]Subscriber{},
	}
	go pw.run()
	return pw, nil
}

func (pw *PollWorker) addNewSubscriber(subscriber Subscriber) {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	log.I("pollWorkers", pw.streamName, "got new subscriber:", subscriber.Id())
	pw.subscribers[subscriber.Id()] = subscriber
	atomic.AddInt32(&pw.numSubscribers, 1)
}

func (pw *PollWorker) removeSubscriber(subscriber Subscriber) {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	log.I("pollWorkers", pw.streamName, "remove subscriber:", subscriber.Id())
	delete(pw.subscribers, subscriber.Id())
	atomic.AddInt32(&pw.numSubscribers, -1)
}

func (pw *PollWorker) Stop() {
	log.I("pollWorkers", pw.streamName, "stopped")
	pw.stopped.Store(true)
}

func (pw *PollWorker) run() {
	log.Info("sub: start polling from", pw.streamName, "@id=", pw.lastSeenOffset)
	for !pw.stopped.Load().(bool) {
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
			for _, value := range pw.subscribers {
				subscriber := value.(Subscriber)
				log.Info("fanout to", subscriber.Id())
				subscriber.OnMessages(pw.streamName, msgs)
			}
			pw.mu.Unlock()
		}
	done:
		time.Sleep(time.Duration(pw.pollIntervalInMs) * time.Millisecond)
	}
	log.D("poll worker stopped")
}

type Hub struct {
	mu          sync.Mutex
	pollWorkers map[string]*PollWorker
	store       Store
	cfg         *Config
}

func NewHub(c *Config) (*Hub, error) {
	store, err := OpenStore(c.DSN)
	if err != nil {
		return nil, err
	}
	return &Hub{
		mu:          sync.Mutex{},
		cfg:         c,
		store:       store,
		pollWorkers: map[string]*PollWorker{},
	}, nil
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
	m.pollWorkers[streamName].addNewSubscriber(subscriber)
	return nil
}

func (m *Hub) Unsubscribe(streamName string, subscriber Subscriber) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if pw, ok := m.pollWorkers[streamName]; ok {
		pw.removeSubscriber(subscriber)
	}
}
