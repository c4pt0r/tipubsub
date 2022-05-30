package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/abiosoft/ishell"
	"github.com/c4pt0r/log"
	"github.com/c4pt0r/tipubsub"
	"github.com/fatih/color"
)

var (
	hub      *tipubsub.Hub
	dsn      = flag.String("dsn", "root:@tcp(localhost:4000)/test", "TiDB DSN")
	logLevel = flag.String("l", "error", "log level")
)

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
		Data: []byte(message),
		Ts:   time.Now().Unix(),
	})
	if err != nil {
		log.Error(err)
		return
	}
	fmt.Println("OK")
}

type Subscriber struct {
	id string
	ch chan tipubsub.Message
}

func NewSubscriber(id string) *Subscriber {
	return &Subscriber{
		id: id,
		ch: make(chan tipubsub.Message),
	}
}

func (s *Subscriber) OnMessages(channelName string, messages []tipubsub.Message) {
	log.Info("on message", messages)
	for _, m := range messages {
		s.ch <- m
	}
}

func (s *Subscriber) getChannel() chan tipubsub.Message {
	return s.ch
}

func (s *Subscriber) ID() string {
	return s.id
}

func sub(channel string, offset int64) {
	subName := fmt.Sprintf("sub-%s-%s", channel, randomString(5))
	s := NewSubscriber(subName)
	fmt.Printf("start listening: %s subscriber id: %s\n",
		color.HiWhiteString(channel),
		color.HiWhiteString(subName))
	err := hub.Subscribe(channel, s, offset)
	if err != nil {
		log.Error(err)
		return
	}
	ch := s.getChannel()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	for {
		select {
		case <-c:
			hub.Unsubscribe(channel, s)
			fmt.Printf("bye\n")
			return
		case m := <-ch:
			fmt.Printf("new message, channel: %s val: %s id=%d\n",
				color.HiWhiteString(channel),
				color.HiWhiteString(string(m.Data)), m.ID)
		}
	}
}

func main() {
	flag.Parse()
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
		Name: "pub",
		Help: "pub <channel> <message>",
		Func: func(c *ishell.Context) {
			if len(c.Args) == 2 {
				pub(c.Args[0], c.Args[1])
			} else {
				c.Println("usage: pub <channel> <message>")
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "sub",
		Help: "sub <channel> [offset]",
		Func: func(c *ishell.Context) {
			if len(c.Args) == 1 {
				sub(c.Args[0], tipubsub.LatestId)
			} else if len(c.Args) == 2 {
				offset, err := strconv.ParseInt(c.Args[1], 10, 64)
				if err != nil {
					c.Println("usage: sub <channel> [offset]")
					return
				}
				sub(c.Args[0], offset)
			} else {
				c.Println("usage: sub <channel> [offset]")
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "gc",
		Help: "gc",
		Func: func(c *ishell.Context) {
			fmt.Println("start force gc...")
			hub.ForceGC()
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
				c.Printf("stream_name\t%s\n", streamName)
				c.Printf("min_id\t%d\n", min)
				c.Printf("max_id\t%d\n", max)
			} else {
				fmt.Println("usage: stat <streamName>")
			}
		},
	})

	shell.Run()
	shell.Close()
}
