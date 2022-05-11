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
	"time"

	"github.com/c4pt0r/log"
)

var (
	LatestId = int64(-1)
)

type Subscriber struct {
	streamName string
	recv       chan []Message
	cfg        *Config
	store      Store

	lastSeenOffset int64
	offset         int64

	maxBatchSize     int
	pollIntervalInMs int
}

func NewSubscriber(cfg *Config, streamName string, offset int64) (*Subscriber, error) {
	s, err := OpenStore(cfg.DSN)
	if err != nil {
		return nil, err
	}
	if offset == LatestId {
		offset, err = s.MaxID(streamName)
		if err != nil {
			return nil, err
		}
	}
	return &Subscriber{
		streamName:       streamName,
		cfg:              cfg,
		recv:             make(chan []Message),
		store:            s,
		lastSeenOffset:   offset,
		maxBatchSize:     cfg.MaxBatchSize,
		pollIntervalInMs: cfg.PollIntervalInMs,
	}, nil
}

func (s *Subscriber) Open() {
	go s.pollWorker()
}

func (s *Subscriber) Receive() <-chan []Message {
	return s.recv
}

func (s *Subscriber) pollWorker() {
	log.Info("sub: start polling from", s.streamName, "@ id=", s.lastSeenOffset)
	for {
		msgs, max, err := s.store.FetchMessages(s.streamName, s.lastSeenOffset, s.maxBatchSize)
		if err != nil {
			log.Error(err)
			time.Sleep(time.Duration(s.pollIntervalInMs) * time.Millisecond)
			goto done
		}
		if len(msgs) > 0 {
			s.lastSeenOffset = max
			s.recv <- msgs
		}
	done:
		time.Sleep(time.Duration(s.pollIntervalInMs) * time.Millisecond)
	}
}
