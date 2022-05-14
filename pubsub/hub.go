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

	subscribers sync.Map
}

func newPollWorker(cfg *Config, s Store, streamName string, offset int64) (*PollWorker, error) {
	var err error
	if offset == LatestId {
		offset, err = s.MaxID(streamName)
		if err != nil {
			return nil, err
		}
	}
	return &PollWorker{
		streamName:       streamName,
		lastSeenOffset:   offset,
		maxBatchSize:     cfg.MaxBatchSize,
		store:            s,
		pollIntervalInMs: cfg.PollIntervalInMs, // FIXME: use config
		subscribers:      sync.Map{},
	}, nil
}

func (pw *PollWorker) addNewSubscriber(subscriber Subscriber) {
	pw.subscribers.Store(subscriber.Id(), subscriber)
}

func (pw *PollWorker) run() {
	log.Info("sub: start polling from", pw.streamName, "@ id=", pw.lastSeenOffset)
	for {
		msgs, max, err := pw.store.FetchMessages(pw.streamName, pw.lastSeenOffset, pw.maxBatchSize)
		if err != nil {
			log.Error(err)
			time.Sleep(time.Duration(pw.pollIntervalInMs) * time.Millisecond)
			goto done
		}
		if len(msgs) > 0 {
			pw.lastSeenOffset = max
			log.Info("sub: got", len(msgs), "messages from", pw.streamName, "@ id=", pw.lastSeenOffset)
			pw.subscribers.Range(func(key, value interface{}) bool {
				subscriber := value.(Subscriber)
				log.Info("fanout to", subscriber.Id())
				subscriber.OnMessages(pw.streamName, msgs)
				return true
			})
		}
	done:
		time.Sleep(time.Duration(pw.pollIntervalInMs) * time.Millisecond)
	}
}

type Hub struct {
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
		cfg:   c,
		store: store,
	}, nil
}

func (m *Hub) Subscribe(streamName string, subscriber Subscriber) error {
	if m.pollWorkers == nil {
		m.pollWorkers = map[string]*PollWorker{}
	}
	// if the stream is not in the map, create a new poll worker for this stream
	if _, ok := m.pollWorkers[streamName]; !ok {
		// create a new poll worker for this stream
		pw, err := newPollWorker(m.cfg, m.store, streamName, LatestId)
		if err != nil {
			return err
		}
		m.pollWorkers[streamName] = pw
		go pw.run()
	}
	m.pollWorkers[streamName].addNewSubscriber(subscriber)
	return nil
}
