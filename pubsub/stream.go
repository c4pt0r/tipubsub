package pubsub

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

func NewStream(cfg *Config, name string) (*Stream, error) {
	s, err := OpenStore(cfg.DSN)
	if err != nil {
		return nil, err
	}
	return &Stream{
		name:         name,
		mq:           make(chan *Message, cfg.MaxBatchSize),
		store:        s,
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

func (s *Stream) Publish(m *Message) int64 {
	if m.Ts == 0 {
		m.Ts = time.Now().UnixNano()
	}
	s.mq <- m
	return m.Ts
}

func (s *Stream) getBatches(maxItems int, maxTimeout time.Duration) chan []*Message {
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
	log.Info("pub: Starting pub worker...")
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
