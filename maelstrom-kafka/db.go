package main

import (
	"context"
	"strconv"
	"strings"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// KV here is more performatic with LinKV since SeqKV is more strict and we don't need sequential consistency here, if using SeqKV it seems by paralelizing
// listCommitedOffsets function performance would be similar
type DB struct {
	KV   *maelstrom.KV
	Node *maelstrom.Node
}

func (db *DB) read(key string) ([]int, error) {
	v, err := db.KV.Read(context.Background(), key)
	if err != nil {
		return []int{}, err
	}

	return desserializeInts(v.(string))
}

func (db *DB) save(key string, msg int) int {
	from, err := db.read(key)
	if err != nil {
		from = []int{}
	}
	to := append(from, msg)

	if err := db.KV.CompareAndSwap(
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

		// No need to paginate results for the workload, but would likely need for something in the real-world
		ret[k] = [][]int{}
		for i := offset; i < total; i++ {
			ret[k] = append(ret[k], []int{i, logs[i]})
		}
	}
	return ret
}

func (db *DB) commitOffsets(to_commit map[string]int) {
	for k, v := range to_commit {
		key := k + "_offset"
		db.KV.Write(context.Background(), key, v)
	}
}

func (db *DB) listCommittedOffsets(keys []string) map[string]int {
	ret := map[string]int{}
	for _, k := range keys {
		key := k + "_offset"
		v, err := db.KV.Read(context.Background(), key)
		if err != nil {
			continue
		}

		offset, _ := strconv.Atoi(v.(string))
		ret[k] = offset
	}

	return ret
}

func NewDB(n *maelstrom.Node, kv *maelstrom.KV) DB {
	return DB{
		Node: n,
		KV:   kv,
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

	var err error
	for i, s := range strs {
		if ints[i], err = strconv.Atoi(s); err != nil {
			return []int{}, err
		}
	}

	return ints, nil
}
