package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Message struct {
	Type    string
	MsgId   int `json:"msg_id,omitempty"`
	Message int
}

type Topology struct {
	Type     string
	Topology map[string][]string
}

type Gossip struct {
	Src   string
	Value int
}

type GossipsMsg struct {
	Type    string
	Gossips []Gossip
}

var values []int
var toGossip []Gossip
var neighbors []string
var seen map[string]bool
var mu sync.Mutex

func gossip(n *maelstrom.Node) {
	t := time.NewTicker(100 * time.Millisecond)
	for range t.C {
		var messages []Gossip
		messages, toGossip = toGossip, []Gossip{}
		for _, neighbor := range neighbors {
			go sendGossips(n, neighbor, messages)
		}
	}
}

func sendGossips(n *maelstrom.Node, neighbor string, gossips []Gossip) {
	g := GossipsMsg{
		Type:    "gossip",
		Gossips: gossips,
	}

	// Just some silly way to handle network partition, if it timeouts in 500ms we try again
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := n.SyncRPC(ctx, neighbor, g); err != nil {
		sendGossips(n, neighbor, gossips)
	}
}

func handleMessage(msg Message, id string) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := seen[id]; ok {
		return
	}
	values = append(values, msg.Message)
	toGossip = append(toGossip, Gossip{Src: id, Value: msg.Message})
	seen[id] = true
}

func main() {
	n := maelstrom.NewNode()
	mu = sync.Mutex{}
	seen = map[string]bool{}
	toGossip = []Gossip{}
	neighbors = []string{}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body Message
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		id := n.ID() + "_" + strconv.Itoa(body.MsgId)
		handleMessage(body, id)

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = values

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body Topology
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		neighbors = body.Topology[n.ID()]
		res := map[string]string{"type": "topology_ok"}
		return n.Reply(msg, res)
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var gossip GossipsMsg
		if err := json.Unmarshal(msg.Body, &gossip); err != nil {
			return err
		}

		mu.Lock()
		for _, a := range gossip.Gossips {
			// It has already passed here, let's not propagate
			if _, ok := seen[a.Src]; ok {
				continue
			}
			toGossip = append(toGossip, a)
			values = append(values, a.Value)
			seen[a.Src] = true
		}
		mu.Unlock()

		return nil
	})

	// keep gossiping as we accumulate messages
	go gossip(n)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
