package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Gossip struct {
	Type    string
	Message int
}

var values []int
var neighbors []string

func gossip(n *maelstrom.Node, message int) {
	for _, neighbor := range neighbors {
		go sendGossip(n, neighbor, message)
	}
}

func sendGossip(n *maelstrom.Node, neighbor string, message int) {
	m := map[string]any{
		"type":    "gossip",
		"message": message,
	}

	// Just some silly way to handle network partition, if it timeouts in 500ms we try again
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	if _, err := n.SyncRPC(ctx, neighbor, m); err != nil {
		sendGossip(n, neighbor, message)
	}
}

func main() {
	n := maelstrom.NewNode()
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "broadcast_ok"
		val := int(body["message"].(float64))
		values = append(values, val)
		go gossip(n, val)
		delete(body, "message")

		return n.Reply(msg, body)
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
		res := map[string]string{"type": "topology_ok"}
		return n.Reply(msg, res)
	})

	n.Handle("init", func(msg maelstrom.Message) error {
		for _, node := range n.NodeIDs() {
			if node != n.ID() {
				neighbors = append(neighbors, node)
			}
		}

		return nil
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body Gossip
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		values = append(values, body.Message)

		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
