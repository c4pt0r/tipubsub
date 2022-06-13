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

func main() {
	flag.Parse()
	cfg := tipubsub.MustLoadConfig(*configFile)
	log.Info("config:", cfg)

	var offset tipubsub.Offset
	var err error
	if *offsetID == "HEAD" {
		offset = tipubsub.LatestId
	} else {
		o, err := strconv.ParseInt(*offsetID, 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		offset = tipubsub.Offset(o)
	}
	hub, err := tipubsub.NewHub(cfg)
	if err != nil {
		log.Fatal(err)
	}
	ch, err := hub.Subscribe(*streamName, "subscriber1")
	if err != nil {
		log.Fatal(err)
	}
	msgs, err := hub.MessagesSinceOffset(*streamName, offset)
	if err != nil {
		log.Fatal(err)
	}
	for _, msg := range msgs {
		log.Info("msg:", msg)
	}
	for msg := range ch {
		log.Info("msg:", msg)
	}
}
