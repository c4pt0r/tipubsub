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
	"strconv"

	"github.com/c4pt0r/tipubsub"

	"github.com/c4pt0r/log"
)

var (
	configFile = flag.String("c", "config.toml", "config file")
	offsetID   = flag.String("offset", "HEAD", "offset")
	streamName = flag.String("s", "test_stream", "stream name")
)

type MySubscriber struct {
}

var _ tipubsub.Subscriber = (*MySubscriber)(nil)

func (s *MySubscriber) OnMessages(streamName string, msgs []tipubsub.Message) {
	for _, msg := range msgs {
		log.E("Got Message:", msg, msg.ID)
	}
}

func (s *MySubscriber) Id() string {
	return "test_subscriber"
}

func main() {
	flag.Parse()
	cfg := tipubsub.MustLoadConfig(*configFile)
	log.Info("config:", cfg)

	var offset int64
	var err error
	if *offsetID == "HEAD" {
		offset = tipubsub.LatestId
	} else {
		offset, err = strconv.ParseInt(*offsetID, 10, 64)
		if err != nil {
			log.Fatal(err)
		}
	}
	sub := &MySubscriber{}
	hub, err := tipubsub.NewHub(cfg)
	if err != nil {
		log.Fatal(err)
	}
	err = hub.Subscribe(*streamName, sub, offset)
	if err != nil {
		log.Fatal(err)
	}
	<-make(chan struct{})
}
