# Development notes

## CPU

```sh
sudo apt update && sudo apt install stress
```

```sh
cargo run --bin pu -- -vvv -- stress --cpu 2 --timeout 10s
```

## GPU
```sh
sudo apt update && sudo apt install gpu-burn nvtop
```

```sh
cargo run --bin pu -- --nvml -vvv -- gpu-burn 6
```

Use `nvtop` to monitor