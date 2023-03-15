package main

import (
	"context"
	"fmt"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type DB struct {
	n     *maelstrom.Node
	nodes []string
	mu    sync.Mutex

	data map[int]int
}

func (db *DB) write(op []any) {
	key := int(op[1].(float64))
	val := int(op[2].(float64))
	db.data[key] = val
}

func (db *DB) syncWrites(writes [][]any) {
	for _, node := range db.nodes {
		txn := Transaction{
			Type: "sync",
			Txn:  writes,
		}
		if _, err := db.n.SyncRPC(context.Background(), node, txn); err != nil {
			// silly error handling, should retry in err here, but not needed for challenge
			panic("error syncing")
		}
	}
}

func (db *DB) AddNode(node string) {
	db.nodes = append(db.nodes, node)
}

func (db *DB) HandleSync(writes [][]any) {
	// Ensure no concurrent writes to internal map
	db.mu.Lock()
	for _, w := range writes {
		db.write(w)
	}
	db.mu.Unlock()
}

func (db *DB) HandleTransaction(txn Transaction) ([][]any, error) {
	writes := [][]any{}

	// Ensure no concurrent writes to internal map
	db.mu.Lock()
	for _, op := range txn.Txn {
		switch op[0].(string) {
		case "r":
			if val, ok := db.data[int(op[1].(float64))]; ok {
				op[2] = val
			}
		case "w":
			writes = append(writes, op)
			db.write(op)
		default:
			return nil, fmt.Errorf("unknown operation")
		}
	}
	db.mu.Unlock()

	go db.syncWrites(writes)
	return txn.Txn, nil
}

func NewDB(n *maelstrom.Node) DB {
	return DB{
		n:     n,
		mu:    sync.Mutex{},
		nodes: []string{},
		data:  map[int]int{},
	}
}
