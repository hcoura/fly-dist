package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Transaction struct {
	Type string
	Id   int
	Txn  [][]any
}

func main() {
	n := maelstrom.NewNode()
	db := NewDB(n)

	n.Handle("txn", func(msg maelstrom.Message) error {
		var txn Transaction
		if err := json.Unmarshal(msg.Body, &txn); err != nil {
			return err
		}

		result, err := db.HandleTransaction(txn)
		if err != nil {
			return err
		}

		res := map[string]any{
			"type": "txn_ok",
			"txn":  result,
		}

		return n.Reply(msg, res)
	})

	n.Handle("sync", func(msg maelstrom.Message) error {
		var txn Transaction
		if err := json.Unmarshal(msg.Body, &txn); err != nil {
			return err
		}

		db.HandleSync(txn.Txn)

		return n.Reply(msg, map[string]any{"type": "sync_ok"})
	})

	n.Handle("init", func(msg maelstrom.Message) error {
		for _, node := range n.NodeIDs() {
			if node != n.ID() {
				db.AddNode(node)
			}
		}

		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
