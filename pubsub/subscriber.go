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
