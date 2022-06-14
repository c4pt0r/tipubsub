package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/abiosoft/ishell"
	"github.com/c4pt0r/log"
	"github.com/c4pt0r/tipubsub"
	"github.com/fatih/color"
)

var (
	hub               *tipubsub.Hub
	dsn               = flag.String("dsn", "root:@tcp(localhost:4000)/test", "TiDB DSN")
	logLevel          = flag.String("l", "error", "log level")
	PrintSampleConfig = flag.Bool("sample-config", false, "print sample config")
)

func printSimpleTable(keys []string, values []interface{}) {
	maxLen := 0
	for _, key := range keys {
		if len(key) > maxLen {
			maxLen = len(key)
		}
	}
	for i, key := range keys {
		fmt.Printf("%s%s\t%v\n", key, strings.Repeat(" ", maxLen-len(key)), values[i])
	}
}

func randomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func pub(channel string, message string) {
	err := hub.Publish(channel, &tipubsub.Message{
		Data: message,
		Ts:   time.Now().UnixNano(),
	})
	if err != nil {
		log.Error(err)
		return
	}
	fmt.Println("OK")
}

func sub(channel string, offset tipubsub.Offset) {
	subName := fmt.Sprintf("sub-%s-%s", channel, randomString(5))
	fmt.Printf("start listening: %s subscriber id: %s at: %v\n",
		color.GreenString(channel),
		color.GreenString(subName),
		offset)
	if offset != tipubsub.LatestId {
		msgs, err := hub.MessagesSinceOffset(channel, offset)
		if err != nil {
			log.Error(err)
			return
		}
		for _, msg := range msgs {
			fmt.Println(msg)
		}
	}
	ch, err := hub.Subscribe(channel, subName)
	if err != nil {
		log.Error(err)
		return
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	for {
		select {
		case <-c:
			goto L
		case m := <-ch:
			fmt.Println(m)
		}
	}
L:
	hub.Unsubscribe(channel, subName)
}

func main() {
	flag.Parse()
	if *PrintSampleConfig {
		tipubsub.PrintSampleConfig()
		return
	}
	log.SetLevelByString(*logLevel)

	cfg := tipubsub.DefaultConfig()
	cfg.DSN = *dsn

	var err error
	hub, err = tipubsub.NewHub(cfg)
	if err != nil {
		log.Fatal(err)
	}
	// set shell prompts
	shell := ishell.New()
	shell.SetPrompt("tipubsub> ")

	// register commands
	shell.AddCmd(&ishell.Cmd{
		Name: "publish",
		Help: "publish <channel> <message>",
		Func: func(c *ishell.Context) {
			if len(c.Args) == 2 {
				pub(c.Args[0], c.Args[1])
			} else {
				c.Println("usage: pub <channel> <message>")
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "subscribe",
		Help: "subscribe <channel> [offset]",
		Func: func(c *ishell.Context) {
			if len(c.Args) == 1 {
				sub(c.Args[0], tipubsub.LatestId)
			} else if len(c.Args) == 2 {
				offset, err := strconv.ParseInt(c.Args[1], 10, 64)
				if err != nil {
					c.Println("usage: subscribe <channel> [offset]")
					return
				}
				sub(c.Args[0], tipubsub.Offset(offset))
				c.Println("OK")
			} else {
				c.Println("usage: subscribe <channel> [offset]")
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "gc",
		Help: "gc",
		Func: func(c *ishell.Context) {
			fmt.Println("start force gc...")
			if len(c.Args) != 1 {
				c.Println("usage: gc <channel>")
				return
			}
			err := hub.ForceGC(c.Args[0])
			if err != nil {
				c.Println(err)
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "ls",
		Help: "list all stream names",
		Func: func(c *ishell.Context) {
			names, err := hub.GetStreamNames()
			if err != nil {
				c.Println(err)
				return
			}
			for _, name := range names {
				c.Println(name)
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "exit",
		Help: "exit",
		Func: func(c *ishell.Context) {
			c.Stop()
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "stat",
		Help: "stat",
		Func: func(c *ishell.Context) {
			if len(c.Args) == 1 {
				streamName := c.Args[0]
				min, max, err := hub.MinMaxID(streamName)
				if err != nil {
					c.Println(err)
					return
				}
				keys := []string{"stream_name", "min_id", "max_id"}
				vals := []interface{}{streamName, min, max}
				printSimpleTable(keys, vals)

			} else {
				fmt.Println("usage: stat <streamName>")
			}
		},
	})

	shell.Run()
	shell.Close()
}
