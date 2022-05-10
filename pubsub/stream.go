package pubsub

import (
	"fmt"
	"time"

	"github.com/c4pt0r/log"
)

type Message struct {
	Ts   int64
	ID   string
	Data []byte
}

func (m Message) String() string {
	return fmt.Sprintf("Message{ID: %s, Ts: %d, Data: %s}", m.ID, m.Ts, string(m.Data))
}

type Stream struct {
	name string
	mq   chan Message

	store        Store
	maxBatchSize int
}

var (
	pullTimeout = time.Millisecond * 100
)

func (s *Stream) Name() string {
	return s.name
}

func NewStream(name string, maxBatchSize int) *Stream {
	if _store == nil {
		panic("Store not initialized")
		return nil
	}
	return &Stream{
		name:         name,
		mq:           make(chan Message, maxBatchSize),
		store:        _store,
		maxBatchSize: maxBatchSize,
	}
}

func (s *Stream) Open() error {
	err := s.store.CreateStream(s.name)
	if err != nil {
		return err
	}
	log.Info("PubSub: Open Stream: ", s.name)
	go s.pubWorker()
	return nil
}

func (s *Stream) Publish(m Message) int64 {
	if m.Ts == 0 {
		m.Ts = time.Now().UnixNano()
	}
	s.mq <- m
	return m.Ts
}

func (s *Stream) getBatches(maxItems int, maxTimeout time.Duration) chan []Message {
	batches := make(chan []Message)
	go func() {
		defer close(batches)
		for keepGoing := true; keepGoing; {
			var batch []Message
			expire := time.After(maxTimeout)
			for {
				select {
				case value, ok := <-s.mq:
					if !ok {
						keepGoing = false
						goto done
					}

					batch = append(batch, value)
					if len(batch) == maxItems {
						goto done
					}

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
	log.Info("PubSub: Starting pub worker...")
	batches := s.getBatches(s.maxBatchSize, pullTimeout)
	for batch := range batches {
		// Put batch to store
		err := s.store.PutMessages(s.name, batch)
		if err != nil {
			// Retry?
			log.Error(err)
		}
	}
}
