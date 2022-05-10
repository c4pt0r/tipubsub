package main

import (
	"time"
	"tipubsub/pubsub"

	"github.com/c4pt0r/log"
)

func main() {
	err := pubsub.OpenStore("root:@tcp(localhost:4000)/test")
	if err != nil {
		panic(err)
	}

	subscriber := pubsub.NewSubscriber("test", 0, 100, 500*time.Millisecond)
	subscriber.Open()

	ch := subscriber.Receive()
	for messages := range ch {
		for _, message := range messages {
			log.I(message)
		}
	}
}
