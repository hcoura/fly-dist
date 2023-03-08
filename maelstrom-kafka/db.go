package main

import (
	"context"
	"strconv"
	"strings"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// TODO: run linters and all
// TODO: set up git for the upper folder?

// TODO: refactor this shit
// TODO: why did I choose the KVs?
// TODO: performance (what would be reasonable??)
// TODO: network partition?

const MaxPooledPerRequest = 10

type DB struct {
	Lkv  *maelstrom.KV
	Node *maelstrom.Node
	Skv  *maelstrom.KV
}

func (db *DB) read(key string) ([]int, error) {
	v, err := db.Lkv.Read(context.Background(), key)
	if err != nil {
		return []int{}, err
	}

	return desserializeInts(v.(string))
}

func (db *DB) save(key string, msg int) int {
	from, err := db.read(key)
	if err != nil {
		// TODO: error can be something else
		rpcErr := err.(*maelstrom.RPCError)
		if rpcErr.Code == maelstrom.KeyDoesNotExist {
			from = []int{}
		}
	}
	to := append(from, msg)

	if err := db.Lkv.CompareAndSwap(
		context.Background(),
		key,
		serializeInts(from),
		serializeInts(to), true,
	); err != nil {
		return db.save(key, msg)
	}
	return len(from)
}

func (db *DB) pool(offsets map[string]int) map[string][][]int {
	ret := map[string][][]int{}
	for k, offset := range offsets {
		logs, err := db.read(k)
		if err != nil {
			continue
		}

		total := len(logs)
		if offset > total {
			ret[k] = [][]int{}
			continue
		}

		max := offset + MaxPooledPerRequest
		if max >= total {
			max = total
		}

		ret[k] = [][]int{}
		for i := offset; i < max; i++ {
			ret[k] = append(ret[k], []int{i, logs[i]})
		}
	}
	return ret
}

func (db *DB) commitOffsets(to_commit map[string]int) {
	for k, v := range to_commit {
		key := k + "_offset"
		db.Skv.Write(context.Background(), key, v)
	}
}

func (db *DB) listCommittedOffsets(keys []string) map[string]int {
	ret := map[string]int{}
	for _, k := range keys {
		key := k + "_offset"
		v, err := db.Skv.Read(context.Background(), key)
		if err != nil {
			continue
		}

		offset, _ := strconv.Atoi(v.(string))
		ret[k] = offset
	}

	return ret
}

func NewDB(n *maelstrom.Node, lkv, skv *maelstrom.KV) DB {
	return DB{
		Node: n,
		Lkv:  lkv,
		Skv:  skv,
	}
}

func serializeInts(values []int) string {
	s := ""

	for i, v := range values {
		s += strconv.Itoa(v)
		if i != len(values)-1 {
			s += ","
		}
	}

	return s
}

func desserializeInts(s string) ([]int, error) {
	strs := strings.Split(s, ",")
	ints := make([]int, len(strs))

	// var err error
	for i, s := range strs {
		// TODO: ignoring errors
		// if ints[i], err = strconv.Atoi(s); err != nil {
		// 	return []int{}, err
		// }
		ints[i], _ = strconv.Atoi(s)
	}

	return ints, nil
}
