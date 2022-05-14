// Copyright 2022 Ed Huang<i@huangdx.net>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"tipubsub/pubsub"

	"github.com/c4pt0r/log"
	"github.com/fatih/color"
)

var (
	configFile = flag.String("c", "config.toml", "config file")
	streamName = flag.String("s", "test_stream", "stream name")
)

type MySubscriber struct {
}

var _ pubsub.Subscriber = (*MySubscriber)(nil)

func (s *MySubscriber) OnMessages(streamName string, msgs []pubsub.Message) {
	log.I("received messages:", color.YellowString(streamName), msgs)
}

func (s *MySubscriber) Id() string {
	return "test_subscriber"
}

func main() {
	flag.Parse()
	cfg := pubsub.MustLoadConfig(*configFile)
	log.Info("config:", cfg)

	sub := &MySubscriber{}
	hub, err := pubsub.NewHub(cfg)
	if err != nil {
		log.Fatal(err)
	}

	err = hub.Subscribe(*streamName, sub)
	if err != nil {
		log.Fatal(err)
	}

	<-make(chan struct{})

}
