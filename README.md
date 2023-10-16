# Rustegan
A distributed system challenge by [fly.io](https://fly.io/dist-sys). The name Rustegan is taken from Jon Gjengset's video of him solving this challenge, the link is [here](https://youtu.be/gboGyccRVXI?si=v3B_Q2Am1Cez4JyI)

## How to start
Build the binary:
```bash
cargo build
```

## Test with Maelstrom
[x] Echo
```bash
./maelstrom/maelstrom test -w echo --bin ./target/debug/echo --node-count 1 --time-limit 10
```

[x] Unique ID Generation
```bash
./maelstrom/maelstrom test -w unique-ids --bin ./target/debug/rustegan --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```

[x] Single-Node Broadcast
```bash
./maelstrom/maelstrom test -w broadcast --bin ./target/debug/rustegan --node-count 1 --time-limit 20 --rate 10
```

[x] Multi-Node Broadcast
```bash
./maelstrom/maelstrom test -w broadcast --bin ./target/debug/rustegan --node-count 5 --time-limit 20 --rate 10
```

[x] Fault Tolerant Broadcast
```bash
./maelstrom/maelstrom test -w broadcast --bin ./target/debug/rustegan --node-count 5 --time-limit 20 --rate 10 --nemesis partition
```

[x] Broadcast Efficiency Part 1
```bash
./maelstrom/maelstrom test -w broadcast --bin ./target/debug/rustegan --node-count 25 --time-limit 20 --rate 100 --nemesis partition
```

[x] Broadcast Efficiency Part 2
```bash
./maelstrom/maelstrom test -w broadcast --bin ./target/debug/rustegan --node-count 25 --time-limit 20 --rate 100 --latency 100 --topology tree4
```
