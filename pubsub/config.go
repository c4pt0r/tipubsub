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

package pubsub

import (
	"fmt"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	DSN              string `toml:"dsn" env:"DSN" env-default:"root:@tcp(localhost:4000)/test"`
	MaxBatchSize     int    `toml:"max_batch_size" env:"MAX_BATCH_SIZE" env-default:"100"`
	PollIntervalInMs int    `toml:"poll_interval_in_ms" env:"POLL_INTERVAL_IN_MS" env-default:"100"`
	GCIntervalInSec  int    `toml:"gc_interval_in_sec" env:"GC_INTERVAL_IN_SEC" env-default:"10"`
}

func (c *Config) String() string {
	return fmt.Sprintf("%+v", *c)
}

// LoadConfig loads config from file.
func LoadConfig(path string) (*Config, error) {
	var cfg Config
	// read configuration from the file and environment variables
	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		err = cleanenv.ReadEnv(&cfg)
		if err != nil {
			return nil, err
		} else {
			return &cfg, nil
		}
	}
	return &cfg, nil
}

// LoadConfigFromEnv loads config from environment variables.
func MustLoadConfig(path string) *Config {
	cfg, err := LoadConfig(path)
	if err != nil {
		panic(err)
	}
	return cfg
}
