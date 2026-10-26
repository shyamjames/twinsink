# Phase 0 Spike: Multi-Output Audio Routing with PipeWire

## Objective
Validate manual creation of a virtual PipeWire sink, routing its monitor ports to two physical sinks simultaneously, setting it as default, and evaluating latency characteristics (wired vs Bluetooth).

## Commands

### 1. Create Virtual Null Sink
Using `pactl` (pipewire-pulse module):
```bash
pactl load-module module-null-sink sink_name=twinsink sink_properties=device.description="TwinSink"
```
Or natively via `pw-cli`:
```bash
pw-cli create-node adapter '{
  factory.name=support.null-audio-sink
  node.name=twinsink
  node.description="TwinSink"
  media.class=Audio/Sink
  audio.position=[FL,FR]
  object.linger=true
}'
```

### 2. Inspect Available Sinks and Ports
```bash
# List available sink playback inputs
pw-link -i

# List available monitor outputs
pw-link -o | grep twinsink
```
Expected monitor output ports:
- `twinsink:monitor_FL`
- `twinsink:monitor_FR`

### 3. Link Monitor Outputs to Two Sinks
Connecting `twinsink` monitor outputs to Sink A (`Headphones`) and Sink B (`HDMI1`):
```bash
# Link to Sink A (Headphones)
pw-link twinsink:monitor_FL alsa_output.pci-0000_00_1f.3-platform-skl_hda_dsp_generic.HiFi__Headphones__sink:playback_FL
pw-link twinsink:monitor_FR alsa_output.pci-0000_00_1f.3-platform-skl_hda_dsp_generic.HiFi__Headphones__sink:playback_FR

# Link to Sink B (HDMI)
pw-link twinsink:monitor_FL alsa_output.pci-0000_00_1f.3-platform-skl_hda_dsp_generic.HiFi__HDMI1__sink:playback_FL
pw-link twinsink:monitor_FR alsa_output.pci-0000_00_1f.3-platform-skl_hda_dsp_generic.HiFi__HDMI1__sink:playback_FR
```

### 4. Set Virtual Sink as System Default
```bash
# Using pactl:
pactl set-default-sink twinsink

# Or using wpctl:
wpctl set-default <twinsink-id>
```

### 5. Playback Verification
Verify simultaneous playback across both outputs:
```bash
pw-play /usr/share/sounds/alsa/Front_Center.wav
```
Or generate and play a raw stereo test tone:
```bash
python3 -c "import math, struct, sys; [sys.stdout.buffer.write(struct.pack('<hh', int(3000*math.sin(2*math.pi*440*i/48000)), int(3000*math.sin(2*math.pi*440*i/48000)))) for i in range(48000)]" | pw-play -a --rate 48000 --channels 2 --format s16 -
```

### 6. Cleanup / Restore
```bash
# Restore original default sink
pactl set-default-sink alsa_output.pci-0000_00_1f.3-platform-skl_hda_dsp_generic.HiFi__Headphones__sink

# Remove links / unload module
pactl unload-module <module-id>
```

## Latency Observations (Wired vs Bluetooth)

1. **Wired Outputs (Analog 3.5mm, USB DAC, HDMI):**
   - PipeWire operates synchronously with internal quantum (typically 512 or 1024 samples @ 48kHz, ~10.6ms - 21.3ms buffer).
   - Direct graph links between the virtual sink monitor and multiple wired outputs run in sync with negligible skew (< 1ms).
   - No audible phase or comb filtering between two synchronized wired outputs.

2. **Bluetooth Outputs (A2DP: SBC / AAC / LDAC):**
   - Wireless transport, packetization, and codec encoding/decoding introduce latency:
     - SBC / AAC: ~120ms to 250ms latency.
     - LDAC / aptX: ~80ms to 150ms latency.
     - aptX-LL / LC3: ~30ms to 50ms latency.
   - When pairing a wired sink with a Bluetooth sink via direct PipeWire links, the wired output plays immediately while the Bluetooth output lags significantly, creating an audible echo/delay (~150ms disparity).
   - PipeWire link-level connection does not automatically compensate for device-specific external presentation latencies.

3. **Implications for `twinsink` Architecture:**
   - Simple PipeWire link routing works well for homogeneous low-latency devices (Phase 3).
   - To support heterogeneous pairs (e.g. Wired + Bluetooth or two Bluetooth headsets with different codecs), `twinsink` must provide an explicit per-device delay pipeline (Phase 6) using an internal stream tap and ring buffer to align audio delivery.
