package main

import (
	"fmt"
	"time"
	"tipubsub/pubsub"
)

func main() {
	err := pubsub.OpenStore("root:@tcp(localhost:4000)/test")
	if err != nil {
		panic(err)
	}

	stream := pubsub.NewStream("test", 100)
	stream.Open()

	for i := 0; i < 500; i++ {
		stream.Publish(pubsub.Message{
			Data: []byte(fmt.Sprintf("Message: %d", i)),
		})
	}

	time.Sleep(5 * time.Second)
}
