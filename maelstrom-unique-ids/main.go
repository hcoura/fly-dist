package main

import (
	"encoding/json"
	"log"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func generateId() string {
	// uuids are very unlikely to ever collide
	id := uuid.New()
	return id.String()
}

func main() {
	n := maelstrom.NewNode()
	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = generateId()

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
