package pubsub

import (
	"fmt"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	DSN              string `toml:"dsn" env:"DSN" env-default:"root:@tcp(localhost:4000)/test"`
	MaxBatchSize     int    `toml:"max_batch_size" env:"MAX_BATCH_SIZE" env-default:"100"`
	PollIntervalInMs int    `toml:"poll_interval_in_ms" env:"POLL_INTERVAL_IN_MS" env-default:"100"`
}

func (c *Config) String() string {
	return fmt.Sprintf("%+v", *c)
}

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

func MustLoadConfig(path string) *Config {
	cfg, err := LoadConfig(path)
	if err != nil {
		panic(err)
	}
	return cfg
}
