# AGENTS.md

Context and operational guidelines for agentic assistants working on `twinsink`.

## Overview

`twinsink` is a headless multi-output audio router for Linux written in Rust. It creates a virtual PipeWire null-sink (`twinsink`), sets it as the default output sink via PipeWire metadata, captures the monitor audio into a custom ring buffer pipeline, and routes audio to two selected physical output sinks with per-device delay compensation, volume scaling, mute controls, and channel modes.

Clients control the daemon via a local Unix domain socket using JSON-lines IPC.

## Architecture

- **`core/` (`twinsink_core`)**: Pure data crate. Contains configuration structures, domain models (`AudioDevice`, `ChannelMode`, `DaemonStatus`), and IPC message definitions (`IpcRequest`, `IpcResponse`, `DaemonEvent`). No I/O or PipeWire dependencies.
- **`daemon/` (`twinsink-daemon`)**:
  - `main.rs`: Tokio runtime entry point, tracing setup (`RUST_LOG` env filter defaulting to `warn`), Unix signal handling.
  - `pipewire_thread.rs`: Dedicated OS thread running PipeWire `MainLoopRc`. Manages registry hotplug events, default sink metadata restoration on startup/crash/exit, virtual null sink adapter, monitor capture stream, and two hardware playback streams.
  - `pipeline.rs`: Software audio pipeline (`AudioPipeline`) with an interleaved float32 ring buffer (240k frames / 5s @ 48kHz). Handles volume, mute, delay lines, channel downmix/split, and underrun-resilient cushion prebuffering.
  - `ipc.rs`: Async Unix domain socket server handling requests and streaming live broadcast events.
  - `config.rs`: Persists active device pairs and audio settings to `~/.config/twinsink/config.toml`.
- **`cli/` (`twinsink`)**: Command-line client interfacing with `$XDG_RUNTIME_DIR/twinsink.sock`.
- **`tui/` (`twinsink-tui`)**: Interactive Ratatui dashboard client subscribed to daemon IPC events.
- **`systemd/`**: User unit file `twinsink.service`.

## Safety & System Rules

1. **Audio Preservation Guarantee**: Never leave the system without a working default sink. The daemon stores the original default sink on start and restores it on SIGINT/SIGTERM or clean shutdown. On startup, it checks if a stale `twinsink` sink is set as default from a prior crash and restores a sane hardware default.
2. **Single-Threaded PipeWire Loop**: `pipewire-rs` types are generally not `Send`. All direct PipeWire operations must remain inside the dedicated thread in `pipewire_thread.rs`. Communicate with Tokio via `pw::channel` or unbounded channels.
3. **Audio Pipeline Thread-Safety**: `AudioPipeline` is protected by `Arc<Mutex<AudioPipeline>>` and accessed within PipeWire realtime process callbacks. Keep locks minimal and avoid allocations or blocking calls inside stream process handlers.

## Build, Test, and Quality Commands

- Build workspace: `cargo build --workspace`
- Run unit tests: `cargo test --workspace`
- Enforce code style: `cargo fmt --check`
- Strict linter checks (zero warnings allowed): `cargo clippy --workspace -- -D warnings`
- Run daemon: `cargo run --bin twinsink-daemon`
- Run CLI: `cargo run --bin twinsink -- <subcommand>`
- Run TUI: `cargo run --bin twinsink-tui`
