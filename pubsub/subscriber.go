package pubsub

import (
	"time"

	"github.com/c4pt0r/log"
)

type Subscriber struct {
	streamName string
	recv       chan []Message
	cfg        *Config
	store      Store

	lastSeen int64
	offset   int64

	maxBatchSize     int
	pollIntervalInMs int
}

func NewSubscriber(cfg *Config, streamName string, offset int64) (*Subscriber, error) {
	s, err := OpenStore(cfg.DSN)
	if err != nil {
		return nil, err
	}
	if offset == 0 {
		// TODO: use last seen from store
		offset, err = s.MaxTs(streamName)
		if err != nil {
			// should not be here
			return nil, err
		}
	}
	return &Subscriber{
		streamName:       streamName,
		cfg:              cfg,
		recv:             make(chan []Message),
		store:            s,
		lastSeen:         offset,
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
	log.Info("sub: start polling from", s.streamName, "@", s.lastSeen)
	for {
		msgs, max, err := s.store.FetchMessages(s.streamName, s.lastSeen, s.maxBatchSize)
		if err != nil {
			log.Error(err)
			time.Sleep(time.Duration(s.pollIntervalInMs) * time.Millisecond)
			goto done
		}
		if len(msgs) > 0 {
			s.lastSeen = max
			s.recv <- msgs
		}
	done:
		time.Sleep(time.Duration(s.pollIntervalInMs) * time.Millisecond)
	}

}
