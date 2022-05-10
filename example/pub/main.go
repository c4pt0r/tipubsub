package main

import (
	"flag"
	"fmt"
	"time"
	"tipubsub/pubsub"

	"github.com/c4pt0r/log"
)

var (
	configFile = flag.String("c", "config.toml", "config file")
)

func main() {
	flag.Parse()
	cfg := pubsub.MustLoadConfig(*configFile)
	log.Info("config:", cfg)

	stream, err := pubsub.NewStream(cfg, "test_stream")
	if err != nil {
		log.Fatal(err)
	}

	if err := stream.Open(); err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 500; i++ {
		stream.Publish(pubsub.Message{
			Data: []byte(fmt.Sprintf("Message: %d", i)),
		})
	}

	time.Sleep(5 * time.Second)
}
