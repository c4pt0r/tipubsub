package pubsub

import (
	"time"

	"github.com/c4pt0r/log"
)

type Subscriber struct {
	streamName string
	recv       chan []Message

	lastSeen int64
	offset   int64
	store    Store

	maxBatchSize int
	pollInterval time.Duration
}

func NewSubscriber(streamName string, lastSeen int64, maxBatchSize int, pollInterval time.Duration) *Subscriber {
	if _store == nil {
		panic("store not initialized")
	}

	if maxBatchSize <= 0 {
		maxBatchSize = 100
	}

	if pollInterval <= 0 {
		pollInterval = 100 * time.Microsecond
	}

	return &Subscriber{
		streamName:   streamName,
		recv:         make(chan []Message),
		store:        _store,
		lastSeen:     lastSeen,
		maxBatchSize: maxBatchSize,
		pollInterval: pollInterval,
	}
}

func (s *Subscriber) Open() error {
	go s.pollWorker()
	return nil
}

func (s *Subscriber) Receive() <-chan []Message {
	return s.recv
}

func (s *Subscriber) pollWorker() {
	var err error
	if s.lastSeen == 0 {
		// TODO: use last seen from store
		s.lastSeen, err = s.store.MaxTs(s.streamName)
		if err != nil {
			// should not be here
			log.Fatal(err)
		}
	}
	log.Info("Start polling from", s.streamName, " ", s.lastSeen)
	for {
		msgs, max, err := s.store.FetchMessages(s.streamName, s.lastSeen, s.maxBatchSize)
		if err != nil {
			log.Error(err)
			time.Sleep(5 * s.pollInterval)
			goto done
		}
		if len(msgs) > 0 {
			s.lastSeen = max
			s.recv <- msgs
		}
	done:
		time.Sleep(s.pollInterval)
	}

}
