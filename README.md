# Rustegan
A distributed system challenge by [fly.io](https://fly.io/dist-sys). The name Rustegan is taken from Jon Gjengset's video of him solving this challenge, the link is [here](https://youtu.be/gboGyccRVXI?si=v3B_Q2Am1Cez4JyI)

## How to start
Build the binary:
```bash
cargo build
```

## Test with Maelstrom
- [x] Echo
```bash
./maelstrom/maelstrom test -w echo --bin ./target/debug/echo --node-count 1 --time-limit 10
```

- [x] Unique ID Generation
```bash
./maelstrom/maelstrom test -w unique-ids --bin ./target/debug/unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```

- [x] Single-Node Broadcast
```bash
./maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 1 --time-limit 20 --rate 10
```

- [x] Multi-Node Broadcast
```bash
./maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 5 --time-limit 20 --rate 10
```

- [x] Fault Tolerant Broadcast
```bash
./maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition
```

- [x] Broadcast Efficiency Part 1
```bash
./maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 25 --time-limit 20 --rate 100 --nemesis partition
```

- [x] Broadcast Efficiency Part 2
```bash
./maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100 --topology tree4
```

- [x] Grow-only Counter
```bash
./maelstrom/maelstrom test -w g-counter --bin ./target/debug/counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition
```

- [x] PN Counter
```bash
./maelstrom/maelstrom test -w pn-counter --bin ./target/debug/counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition
```

- [x] Single-node Kafka
```bash
./maelstrom/maelstrom test -w kafka --bin ./target/debug/kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
```

- [x] Multi-node Kafka
```bash
./maelstrom/maelstrom test -w kafka --bin ./target/debug/multinode-kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
```

- [x] Single-node Total Available Transactions
```bash
./maelstrom/maelstrom test -w txn-rw-register --bin ./target/debug/txn-rw-register --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
```

- [x] Multi-node Totally-available Read Uncommitted Transactions
```bash
./maelstrom/maelstrom test -w txn-rw-register --bin ./target/debug/txn-rw-uncommitted --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition
```

- [x] Multi-node Totally-available Read Committed Transactions
```bash
./maelstrom/maelstrom test -w txn-rw-register --bin ./target/debug/txn-rw-committed --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total –-nemesis partition
```
