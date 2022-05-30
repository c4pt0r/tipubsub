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
	"fmt"
	"time"

	"github.com/c4pt0r/log"
)

type Message struct {
	ID   int64 // auto generated
	Ts   int64
	Data []byte
}

func (m Message) String() string {
	return fmt.Sprintf("Message{ID: %d, Ts: %d, Data: %s}", m.ID, m.Ts, string(m.Data))
}

type Stream struct {
	name string
	mq   chan *Message

	store        Store
	maxBatchSize int
}

var (
	pullTimeout = time.Millisecond * 100
)

func (s *Stream) Name() string {
	return s.name
}

func NewStream(cfg *Config, s Store, name string) (*Stream, error) {
	return &Stream{
		store:        s,
		name:         name,
		mq:           make(chan *Message, cfg.MaxBatchSize),
		maxBatchSize: cfg.MaxBatchSize,
	}, nil
}

func (s *Stream) Open() error {
	err := s.store.CreateStream(s.name)
	if err != nil {
		return err
	}
	log.Info("pub: open stream:", s.name)
	go s.pubWorker()
	return nil
}

func (s *Stream) Publish(m *Message) {
	if m.Ts == 0 {
		m.Ts = time.Now().UnixNano()
	}
	s.mq <- m
}

func (s *Stream) MinMaxID() (int64, int64, error) {
	return s.store.MinMaxID(s.name)
}

func (s *Stream) getBatches(maxItems int, maxTimeout time.Duration) chan []*Message {
	// Create a channel to receive batches
	batches := make(chan []*Message)
	go func() {
		defer close(batches)
		for keepGoing := true; keepGoing; {
			var batch []*Message
			expire := time.After(maxTimeout)
			for {
				select {
				case value, ok := <-s.mq:
					if !ok {
						keepGoing = false
						goto done
					}
					batch = append(batch, value)
					// if batch is full, return batch
					if len(batch) == maxItems {
						goto done
					}
				// if channel is empty, block for maxTimeout
				case <-expire:
					goto done
				}
			}
		done:
			if len(batch) > 0 {
				batches <- batch
			}
		}
	}()
	return batches
}

func (s *Stream) pubWorker() {
	log.Info("pub: Starting pub worker...")
	batches := s.getBatches(s.maxBatchSize, pullTimeout)
	for batch := range batches {
		// Put batch to store
		err := s.store.PutMessages(s.name, batch)
		if err != nil {
			// TODO: Retry?
			log.Error(err)
		}
	}
}
