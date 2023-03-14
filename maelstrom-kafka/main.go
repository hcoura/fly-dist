package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type SendMsg struct {
	Type string
	Key  string
	Msg  int
}

type PollMsg struct {
	Type    string
	Offsets map[string]int
}

type ListCommitOffsetMsg struct {
	Type    string
	Offsets []string
}

type CommitOffsetMsg struct {
	Type    string
	Offsets map[string]int
}

func main() {
	n := maelstrom.NewNode()
	lkv := maelstrom.NewLinKV(n)
	db := NewDB(n, lkv)

	n.Handle("send", func(msg maelstrom.Message) error {
		var body SendMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offset := db.save(body.Key, body.Msg)

		res := map[string]any{
			"type":   "send_ok",
			"offset": offset,
		}

		return n.Reply(msg, res)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body PollMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		res := map[string]any{
			"type": "poll_ok",
			"msgs": db.pool(body.Offsets),
		}

		return n.Reply(msg, res)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body CommitOffsetMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		go db.commitOffsets(body.Offsets)

		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body ListCommitOffsetMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		res := map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": db.listCommittedOffsets(body.Offsets),
		}

		return n.Reply(msg, res)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
