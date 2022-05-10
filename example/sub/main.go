package main

import (
	"flag"
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

	subscriber, err := pubsub.NewSubscriber(cfg, "test_stream", pubsub.LatestTs)
	if err != nil {
		log.Fatal(err)
	}

	subscriber.Open()
	ch := subscriber.Receive()
	for messages := range ch {
		for _, message := range messages {
			log.I(message)
		}
	}
}
