# Gossip Glomers solution

These are a series of challenges by fly.io on https://fly.io/dist-sys/

## Challenges

Each challenge, once you `go install` them can be run as described bellow, you can follow the links for more details on the challenge (some are multi step).

### 1. Echo

https://fly.io/dist-sys/1/

```
./maelstrom test -w echo --bin ~/go/bin/maelstrom-echo --node-count 1 --time-limit 10
```

### 2. Unique ID Generation 

https://fly.io/dist-sys/2/

```
./maelstrom test -w unique-ids --bin ~/go/bin/maelstrom-unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```

### 3. Broadcast

https://fly.io/dist-sys/3a/

./maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100

### 4. Grow-Only Counter 

https://fly.io/dist-sys/4/

./maelstrom test -w g-counter --bin ~/go/bin/maelstrom-counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition

### 5. Kafka-Style Log 

https://fly.io/dist-sys/5a/

./maelstrom test -w kafka --bin ~/go/bin/maelstrom-kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000

### 6. Totally-Available Transactions

https://fly.io/dist-sys/6a/

./maelstrom test -w txn-rw-register --bin ~/go/bin/maelstrom-txn --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total –-nemesis partition
