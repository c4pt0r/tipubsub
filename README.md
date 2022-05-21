# tipubsub

a small demo using TiDB as a message queue, providing Sub/Pub API at sacle.

Subscriber:

```
type MySubscriber struct {}

var _ pubsub.Subscriber = (*MySubscriber)(nil)

func (s *MySubscriber) OnMessages(streamName string, msgs []pubsub.Message) {
	for _, msg := range msgs {
		log.I("Got Message:", msg, msg.ID)
	}
}

func (s *MySubscriber) Id() string {
	return "my_subscriber"
}

func main() {
    ...
	sub := &MySubscriber{}
	hub, err := pubsub.NewHub(cfg)
	if err != nil {
		log.Fatal(err)
	}
	err = hub.Subscribe("test_stream", sub, offset)
	if err != nil {
		log.Fatal(err)
	}
    ...
}

```

Publisher:

```
func main() {
    ...
	hub, err := pubsub.NewHub(cfg)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		hub.Publish("test_stream", &pubsub.Message{
			Data: []byte(fmt.Sprintf("Message: %d", i)),
		})
	}
}
```

See `example` for more details
