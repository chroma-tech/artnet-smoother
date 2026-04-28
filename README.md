# Art-Net Smoother

`artnet-smoother` is a one-controller-per-process UDP proxy for smoothing bursty Art-Net output before it reaches Timber controllers using DM9051 Ethernet.

The default mode is `sync-delay`: ArtDmx packets are buffered until ArtSync, then the completed frame is retransmitted evenly across the next frame interval. The matching ArtSync packet is sent after the delayed packet group, preserving sync semantics with a constant one-frame delay.

## Build

```sh
cargo build --release
```

## Example

```sh
artnet-smoother \
  --listen 192.168.8.101:6454 \
  --target 192.168.8.231:6454 \
  --mode sync-delay \
  --stats
```

For multiple local test instances:

```sh
artnet-smoother --listen 0.0.0.0:6454 --target 192.168.8.231:6454
artnet-smoother --listen 0.0.0.0:6455 --target 192.168.8.232:6454
```

## Modes

- `passthrough`: immediately forwards accepted packets.
- `spread`: buffers ArtDmx and spreads each configured FPS interval. Incoming ArtSync is forwarded immediately.
- `sync-delay`: buffers ArtDmx until ArtSync, spreads the completed frame over the next observed frame interval, then forwards the delayed ArtSync.
- `fixed-fps`: buffers ArtDmx and emits at `--fps`, ignoring incoming ArtSync.

## Useful Flags

- `--fps 40`: fallback or fixed frame rate.
- `--stats`: print once-per-second packet and queue stats.
- `--universes 10-14,20-24`: only smooth/forward selected ArtDmx universes.
- `--min-packet-gap-us 500`: enforce a minimum gap between output packets.
- `--max-pps 2000`: derive a minimum output gap from a packet-per-second ceiling.
- `--unknown drop`: drop unknown Art-Net and non-Art-Net packets instead of forwarding them.
