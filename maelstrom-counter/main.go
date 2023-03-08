package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const CounterKey = "counter"

var kv maelstrom.KV

func readCounter() int {
	// TODO: error handling / network partition
	c, err := kv.ReadInt(context.Background(), CounterKey)
	if err == nil {
		return c
	}

	rpcErr := err.(*maelstrom.RPCError)
	if rpcErr.Code == maelstrom.KeyDoesNotExist {
		return 0
	}
	panic(err)
}

func addDelta(delta int) {
	current := readCounter()
	if err := kv.CompareAndSwap(context.Background(), CounterKey, current, current+delta, true); err != nil {
		addDelta(delta)
	}
}

func main() {
	n := maelstrom.NewNode()
	kv = *maelstrom.NewSeqKV(n)
	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "add_ok"
		go addDelta(int(body["delta"].(float64)))

		delete(body, "delta")

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		// spam a bit due to https://github.com/jepsen-io/maelstrom/issues/47
		// on theory there is a "trick" for this...
		body["value"] = readCounter()
		body["value"] = readCounter()

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
