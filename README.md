# TwinSink

**TwinSink** is a headless, multi-output audio router for Linux running on **PipeWire** and **WirePlumber**.

It creates a virtual PipeWire audio sink (`twinsink`), sets it as your system-wide default output, and routes incoming audio simultaneously to exactly **TWO** selected audio output devices (e.g., your laptop speakers + Bluetooth headphones, or wired headphones + external monitor over HDMI).

TwinSink operates entirely over a local Unix domain socket with zero network footprint. It includes a scriptable CLI (`twinsink`) and an interactive terminal user interface (`twinsink-tui`).

---

## Features

- **Dual-Device Routing**: Route system audio to two physical or virtual audio sinks in real-time.
- **Audio Pipeline & Delay Compensation**: Built-in 5-second circular ring buffer with millisecond-precision delay tuning to eliminate Bluetooth latency sync issues.
- **Three Channel Modes**:
  - `duplicate`: Full stereo $(L, R)$ sent to both outputs.
  - `split`: Left $(L, L)$ sent to Device A, Right $(R, R)$ sent to Device B.
  - `mono`: Downmixed $(L + R)/2$ sent to both outputs.
- **Per-Device Volume & Mute**: Independent volume scaling and mute controls with digital gain staging.
- **Automatic Hotplug & Re-linking**: Auto-reconnects streams when Bluetooth devices or USB headsets disconnect and reconnect.
- **Audio Invariant Safety**: Never leaves you stranded without audio. Graceful shutdown (SIGINT/SIGTERM) or startup after a hard crash (`kill -9`) automatically restores your original default sound card.
- **Persistent Configuration**: Device pair, volume, mute, delay, and mode settings automatically persist to `~/.config/twinsink/config.toml`.
- **Interactive TUI**: Built with `ratatui` featuring live gauge meters, delay sliders, and keyboard controls.

---

## 1. Setup & Prerequisites

TwinSink is developed on Arch Linux with PipeWire and WirePlumber.

### Install Required System Packages

Open your terminal and install the required dependencies using `pacman`:

```bash
sudo pacman -S --needed rustup pipewire pipewire-pulse wireplumber clang pkgconf base-devel
```

### Install Stable Rust Toolchain

Configure Rustup to use the stable Rust toolchain:

```bash
rustup default stable
```

---

## 2. Build and Install

From the root of this repository, install the three binaries (`twinsink-daemon`, `twinsink`, `twinsink-tui`) to your Cargo bin directory:

```bash
cargo install --path daemon
cargo install --path cli
cargo install --path tui
```

### Ensure Cargo Bin is in your `PATH`

If you haven't already, add `~/.cargo/bin` to your shell's `PATH`.

For **Bash** (`~/.bashrc`):
```bash
export PATH="$HOME/.cargo/bin:$PATH"
```

For **Zsh** (`~/.zshrc`):
```bash
export PATH="$HOME/.cargo/bin:$PATH"
```

For **Fish** (`~/.config/fish/config.fish`):
```fish
fish_add_path ~/.cargo/bin
```

Reload your shell or run `source ~/.bashrc` (or your shell's rc file).

---

## 3. How to Run

### Step 1: Start the Daemon

Start the background daemon:

```bash
twinsink-daemon &
```

The daemon will:
1. Discover all connected PipeWire audio sinks.
2. Load any saved settings from `~/.config/twinsink/config.toml`.
3. Create the `twinsink` virtual sink and set it as your default output.
4. Listen on `$XDG_RUNTIME_DIR/twinsink.sock`.

### Step 2: Check Status with the CLI

Check the current status and active outputs:

```bash
twinsink status
```

To view all available output sinks on your system:

```bash
twinsink devices
```

To route audio to two specific sinks:

```bash
twinsink select alsa_output.pci-0000_00_1f.3-platform-skl_hda_dsp_generic.HiFi__Speaker__sink bluez_output.XX_XX_XX_XX_XX_XX.1
```

Adjust volume, mute, and delay:

```bash
# Set volume (0.0 to 1.0)
twinsink volume <device-name> 0.80

# Toggle or set mute
twinsink mute <device-name>

# Compensate Bluetooth latency by delaying Device A by 150 ms
twinsink delay <device-name> 150

# Switch routing mode (duplicate, split, or mono)
twinsink mode split
twinsink mode duplicate
```

For JSON output suitable for scripting or waybar/polybar widgets, pass `--json`:

```bash
twinsink status --json
```

### Step 3: Launch the TUI

Launch the interactive terminal interface:

```bash
twinsink-tui
```

#### TUI Keyboard Controls:
| Key | Action |
| :--- | :--- |
| `j` / `k` or `↓` / `↑` | Navigate through available audio sinks |
| `1` | Assign highlighted sink as **Device A** (Primary) |
| `2` | Assign highlighted sink as **Device B** (Secondary) |
| `Tab` | Switch active control focus between Device A and Device B |
| `+` / `-` | Increase / decrease volume by 5% on focused device |
| `x` | Toggle mute on focused device |
| `←` / `→` or `[` / `]` | Decrease / increase delay by 10 ms on focused device |
| `c` | Cycle channel mode (`Duplicate` $\to$ `Split` $\to$ `Mono`) |
| `q` or `Esc` | Quit TUI cleanly |

---

## 4. Autostart

### Option A: systemd User Service (Recommended)

1. Copy the systemd user unit into your user systemd directory:

```bash
mkdir -p ~/.config/systemd/user
cp systemd/twinsink.service ~/.config/systemd/user/
```

2. Reload the systemd daemon, enable, and start the service:

```bash
systemctl --user daemon-reload
systemctl --user enable --now twinsink
```

3. Check service status:

```bash
systemctl --user status twinsink
```

To view live service logs:

```bash
journalctl --user -u twinsink -f
```

### Option B: Niri Wayland Compositor

If you use the **niri** Wayland compositor, add this line to your `~/.config/niri/config.kdl`:

```kdl
spawn-at-startup "twinsink-daemon"
```

---

## 5. Troubleshooting & FAQ

### 1. No sound coming from speakers/headphones

- **Check if twinsink is default**: Run `wpctl status` and look under `Settings -> Default Configured Devices`. `twinsink` should be listed as `Audio/Sink`.
- **Check volume and mute**: Run `twinsink status`. Ensure neither output is muted or set to 0% volume.
- **Check stream linking**: Run `pw-link -l`. Verify that `twinsink:monitor_FL/FR` connects to `twinsink-capture:input_FL/FR` and `twinsink-out-a/b` connects to your physical outputs.

### 2. Device is not listed in `twinsink devices`

- Ensure the device is recognized by PipeWire by checking `wpctl status`.
- For Bluetooth devices, make sure the device is paired, connected, and in A2DP playback mode:
  ```bash
  bluetoothctl info <MAC_ADDRESS>
  ```
- Once connected, TwinSink will automatically detect the new sink and make it available.

### 3. Restoring the default sink manually

If you ever kill the daemon abruptly (`kill -9`) without letting it shut down, you can restore your original hardware sound card manually using `wpctl`:

1. Find your physical sink ID:
   ```bash
   wpctl status
   ```
   Look for your soundcard sink under `Audio -> Sinks` (e.g., `64. 500 Series Chipset Family HD Audio Speaker`).

2. Set it as your default:
   ```bash
   wpctl set-default 64
   ```

Alternatively, you can run the daemon with `--restore-only` to clean up any stale virtual sink references:
```bash
twinsink-daemon --restore-only
```

---

## License

MIT License. See [LICENSE](LICENSE) for details.
