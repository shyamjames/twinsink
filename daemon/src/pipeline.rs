use pipewire::spa;
use twinsink_core::ChannelMode;

pub const SAMPLE_RATE: u32 = 48000;
pub const CHANNELS: usize = 2;
pub const RING_CAPACITY_FRAMES: usize = 240_000; // 5 seconds @ 48kHz
pub const BASE_CUSHION_FRAMES: u64 = 2048; // ~42.6ms cushion to absorb graph jitter

pub struct AudioPipeline {
    ring_buffer: Vec<f32>,
    write_frame: u64,
    read_frame_a: u64,
    read_frame_b: u64,
    buffering_a: bool,
    buffering_b: bool,
    sync_needed_a: bool,
    sync_needed_b: bool,
    pub mode: ChannelMode,
    pub delay_ms_a: u32,
    pub delay_ms_b: u32,
    pub volume_a: f32,
    pub volume_b: f32,
    pub mute_a: bool,
    pub mute_b: bool,
}

impl AudioPipeline {
    pub fn new(
        mode: ChannelMode,
        delay_ms_a: u32,
        delay_ms_b: u32,
        volume_a: f32,
        volume_b: f32,
        mute_a: bool,
        mute_b: bool,
    ) -> Self {
        Self {
            ring_buffer: vec![0.0; RING_CAPACITY_FRAMES * CHANNELS],
            write_frame: 0,
            read_frame_a: 0,
            read_frame_b: 0,
            buffering_a: true,
            buffering_b: true,
            sync_needed_a: false,
            sync_needed_b: false,
            mode,
            delay_ms_a,
            delay_ms_b,
            volume_a,
            volume_b,
            mute_a,
            mute_b,
        }
    }

    pub fn push_raw_bytes(&mut self, bytes: &[u8], n_samples: usize) {
        let n_frames = n_samples / CHANNELS;
        if (bytes.as_ptr() as usize).is_multiple_of(std::mem::align_of::<f32>()) {
            let f32_slice =
                unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const f32, n_samples) };
            for i in 0..n_frames {
                let pos =
                    (((self.write_frame + i as u64) as usize) % RING_CAPACITY_FRAMES) * CHANNELS;
                self.ring_buffer[pos] = f32_slice[i * 2];
                self.ring_buffer[pos + 1] = f32_slice[i * 2 + 1];
            }
        } else {
            for (i, c) in bytes
                .as_chunks::<4>()
                .0
                .iter()
                .take(n_samples)
                .copied()
                .enumerate()
            {
                let val = f32::from_ne_bytes(c);
                let frame_idx = i / CHANNELS;
                let ch_idx = i % CHANNELS;
                let pos = (((self.write_frame + frame_idx as u64) as usize) % RING_CAPACITY_FRAMES)
                    * CHANNELS
                    + ch_idx;
                self.ring_buffer[pos] = val;
            }
        }
        self.write_frame += n_frames as u64;
    }

    pub fn read_frames_a(&mut self, out_bytes: &mut [u8], n_frames: usize) {
        let delay_frames = (self.delay_ms_a as u64 * SAMPLE_RATE as u64) / 1000;
        let target_delay = BASE_CUSHION_FRAMES + delay_frames;

        if self.sync_needed_a {
            self.read_frame_a = self.write_frame.saturating_sub(target_delay);
            self.buffering_a = true;
            self.sync_needed_a = false;
        }

        let available = self.write_frame.saturating_sub(self.read_frame_a);

        if self.buffering_a {
            if available < target_delay {
                out_bytes.fill(0);
                return;
            }
            self.read_frame_a = self.write_frame.saturating_sub(target_delay);
            self.buffering_a = false;
        }

        let available = self.write_frame.saturating_sub(self.read_frame_a);
        if available == 0 {
            self.buffering_a = true;
            self.read_frame_a = self.write_frame;
            out_bytes.fill(0);
            return;
        }

        if available > RING_CAPACITY_FRAMES as u64 {
            self.read_frame_a = self.write_frame.saturating_sub(target_delay);
        }

        let available = self.write_frame.saturating_sub(self.read_frame_a);
        let frames_to_read = n_frames.min(available as usize);

        let gain = if self.mute_a {
            0.0
        } else {
            self.volume_a.clamp(0.0, 1.0)
        };

        for i in 0..frames_to_read {
            let current_frame = self.read_frame_a + i as u64;
            let pos = ((current_frame as usize) % RING_CAPACITY_FRAMES) * CHANNELS;
            let raw_l = self.ring_buffer[pos];
            let raw_r = self.ring_buffer[pos + 1];

            let (mapped_l, mapped_r) = match self.mode {
                ChannelMode::Duplicate => (raw_l, raw_r),
                ChannelMode::Split => (raw_l, raw_l),
                ChannelMode::Mono => {
                    let m = (raw_l + raw_r) * 0.5;
                    (m, m)
                }
            };

            let final_l = (mapped_l * gain).clamp(-1.0, 1.0);
            let final_r = (mapped_r * gain).clamp(-1.0, 1.0);

            let byte_pos = i * 8;
            out_bytes[byte_pos..byte_pos + 4].copy_from_slice(&final_l.to_ne_bytes());
            out_bytes[byte_pos + 4..byte_pos + 8].copy_from_slice(&final_r.to_ne_bytes());
        }

        if frames_to_read < n_frames {
            out_bytes[frames_to_read * 8..n_frames * 8].fill(0);
            self.read_frame_a = self.write_frame;
            self.buffering_a = true;
        } else {
            self.read_frame_a += frames_to_read as u64;
        }
    }

    pub fn read_frames_b(&mut self, out_bytes: &mut [u8], n_frames: usize) {
        let delay_frames = (self.delay_ms_b as u64 * SAMPLE_RATE as u64) / 1000;
        let target_delay = BASE_CUSHION_FRAMES + delay_frames;

        if self.sync_needed_b {
            self.read_frame_b = self.write_frame.saturating_sub(target_delay);
            self.buffering_b = true;
            self.sync_needed_b = false;
        }

        let available = self.write_frame.saturating_sub(self.read_frame_b);

        if self.buffering_b {
            if available < target_delay {
                out_bytes.fill(0);
                return;
            }
            self.read_frame_b = self.write_frame.saturating_sub(target_delay);
            self.buffering_b = false;
        }

        let available = self.write_frame.saturating_sub(self.read_frame_b);
        if available == 0 {
            self.buffering_b = true;
            self.read_frame_b = self.write_frame;
            out_bytes.fill(0);
            return;
        }

        if available > RING_CAPACITY_FRAMES as u64 {
            self.read_frame_b = self.write_frame.saturating_sub(target_delay);
        }

        let available = self.write_frame.saturating_sub(self.read_frame_b);
        let frames_to_read = n_frames.min(available as usize);

        let gain = if self.mute_b {
            0.0
        } else {
            self.volume_b.clamp(0.0, 1.0)
        };

        for i in 0..frames_to_read {
            let current_frame = self.read_frame_b + i as u64;
            let pos = ((current_frame as usize) % RING_CAPACITY_FRAMES) * CHANNELS;
            let raw_l = self.ring_buffer[pos];
            let raw_r = self.ring_buffer[pos + 1];

            let (mapped_l, mapped_r) = match self.mode {
                ChannelMode::Duplicate => (raw_l, raw_r),
                ChannelMode::Split => (raw_r, raw_r),
                ChannelMode::Mono => {
                    let m = (raw_l + raw_r) * 0.5;
                    (m, m)
                }
            };

            let final_l = (mapped_l * gain).clamp(-1.0, 1.0);
            let final_r = (mapped_r * gain).clamp(-1.0, 1.0);

            let byte_pos = i * 8;
            out_bytes[byte_pos..byte_pos + 4].copy_from_slice(&final_l.to_ne_bytes());
            out_bytes[byte_pos + 4..byte_pos + 8].copy_from_slice(&final_r.to_ne_bytes());
        }

        if frames_to_read < n_frames {
            out_bytes[frames_to_read * 8..n_frames * 8].fill(0);
            self.read_frame_b = self.write_frame;
            self.buffering_b = true;
        } else {
            self.read_frame_b += frames_to_read as u64;
        }
    }

    pub fn set_delay_a(&mut self, delay_ms: u32) {
        self.delay_ms_a = delay_ms;
        self.sync_needed_a = true;
    }

    pub fn set_delay_b(&mut self, delay_ms: u32) {
        self.delay_ms_b = delay_ms;
        self.sync_needed_b = true;
    }

    pub fn set_mode(&mut self, mode: ChannelMode) {
        self.mode = mode;
    }

    pub fn set_volume_a(&mut self, vol: f32) {
        self.volume_a = vol.clamp(0.0, 1.0);
    }

    pub fn set_volume_b(&mut self, vol: f32) {
        self.volume_b = vol.clamp(0.0, 1.0);
    }

    pub fn set_mute_a(&mut self, mute: bool) {
        self.mute_a = mute;
    }

    pub fn set_mute_b(&mut self, mute: bool) {
        self.mute_b = mute;
    }
}

pub fn make_format_params() -> Vec<u8> {
    let mut audio_info = spa::param::audio::AudioInfoRaw::new();
    audio_info.set_format(spa::param::audio::AudioFormat::F32LE);
    audio_info.set_rate(SAMPLE_RATE);
    audio_info.set_channels(CHANNELS as u32);

    pipewire::spa::pod::serialize::PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &pipewire::spa::pod::Value::Object(pipewire::spa::pod::Object {
            type_: pipewire::spa::utils::SpaTypes::ObjectParamFormat.as_raw(),
            id: pipewire::spa::param::ParamType::EnumFormat.as_raw(),
            properties: audio_info.into(),
        }),
    )
    .unwrap()
    .0
    .into_inner()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_test_stereo_frames(count: usize) -> Vec<f32> {
        let mut frames = Vec::with_capacity(count * 2);
        for i in 0..count {
            // Stay well within [-1.0, 1.0] to prevent clamping
            let l = (i as f32 + 1.0) * 0.0001;
            let r = -((i as f32 + 1.0) * 0.0001);
            frames.push(l);
            frames.push(r);
        }
        frames
    }

    fn frames_to_bytes(frames: &[f32]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(frames.len() * 4);
        for &s in frames {
            bytes.extend_from_slice(&s.to_ne_bytes());
        }
        bytes
    }

    #[test]
    fn test_pipeline_duplicate_mode() {
        let mut pipeline = AudioPipeline::new(ChannelMode::Duplicate, 0, 0, 1.0, 1.0, false, false);
        let test_frames = generate_test_stereo_frames(5000);
        let test_bytes = frames_to_bytes(&test_frames);
        pipeline.push_raw_bytes(&test_bytes, test_frames.len());

        let mut out_a = vec![0u8; 8 * 10]; // 10 frames
        pipeline.read_frames_a(&mut out_a, 10);

        let out_f32: Vec<f32> = out_a
            .chunks_exact(4)
            .map(|c| f32::from_ne_bytes(c.try_into().unwrap()))
            .collect();

        // In duplicate mode, L is positive and R is negative
        assert!(out_f32[0] > 0.0, "Expected positive Left sample");
        assert!(out_f32[1] < 0.0, "Expected negative Right sample");
    }

    #[test]
    fn test_pipeline_split_mode() {
        let mut pipeline = AudioPipeline::new(ChannelMode::Split, 0, 0, 1.0, 1.0, false, false);
        let test_frames = generate_test_stereo_frames(5000);
        let test_bytes = frames_to_bytes(&test_frames);
        pipeline.push_raw_bytes(&test_bytes, test_frames.len());

        let mut out_a = vec![0u8; 8 * 10];
        let mut out_b = vec![0u8; 8 * 10];
        pipeline.read_frames_a(&mut out_a, 10);
        pipeline.read_frames_b(&mut out_b, 10);

        let out_a_f32: Vec<f32> = out_a
            .chunks_exact(4)
            .map(|c| f32::from_ne_bytes(c.try_into().unwrap()))
            .collect();
        let out_b_f32: Vec<f32> = out_b
            .chunks_exact(4)
            .map(|c| f32::from_ne_bytes(c.try_into().unwrap()))
            .collect();

        // Device A receives Left on both channels (both > 0.0)
        assert!(out_a_f32[0] > 0.0);
        assert_eq!(out_a_f32[0], out_a_f32[1]);

        // Device B receives Right on both channels (both < 0.0)
        assert!(out_b_f32[0] < 0.0);
        assert_eq!(out_b_f32[0], out_b_f32[1]);
    }

    #[test]
    fn test_pipeline_mono_mode() {
        let mut pipeline = AudioPipeline::new(ChannelMode::Mono, 0, 0, 1.0, 1.0, false, false);
        // Feed L = 0.4, R = 0.2 -> mono should be 0.3
        let frames: Vec<f32> = [0.4f32, 0.2f32].repeat(5000);
        let bytes = frames_to_bytes(&frames);
        pipeline.push_raw_bytes(&bytes, frames.len());

        let mut out_a = vec![0u8; 8 * 5];
        pipeline.read_frames_a(&mut out_a, 5);

        let out_f32: Vec<f32> = out_a
            .chunks_exact(4)
            .map(|c| f32::from_ne_bytes(c.try_into().unwrap()))
            .collect();

        for sample in out_f32 {
            assert!((sample - 0.3).abs() < 1e-4);
        }
    }

    #[test]
    fn test_pipeline_gain_and_mute() {
        let mut pipeline = AudioPipeline::new(ChannelMode::Duplicate, 0, 0, 0.5, 1.0, false, true);
        let frames: Vec<f32> = [0.4f32, 0.4f32].repeat(5000);
        let bytes = frames_to_bytes(&frames);
        pipeline.push_raw_bytes(&bytes, frames.len());

        let mut out_a = vec![0u8; 8 * 5];
        let mut out_b = vec![0u8; 8 * 5];
        pipeline.read_frames_a(&mut out_a, 5);
        pipeline.read_frames_b(&mut out_b, 5);

        let out_a_f32: Vec<f32> = out_a
            .chunks_exact(4)
            .map(|c| f32::from_ne_bytes(c.try_into().unwrap()))
            .collect();
        let out_b_f32: Vec<f32> = out_b
            .chunks_exact(4)
            .map(|c| f32::from_ne_bytes(c.try_into().unwrap()))
            .collect();

        // Device A volume is 0.5: 0.4 * 0.5 = 0.2
        for sample in out_a_f32 {
            assert!((sample - 0.2).abs() < 1e-4);
        }

        // Device B is muted: all samples must be 0.0
        for sample in out_b_f32 {
            assert_eq!(sample, 0.0);
        }
    }

    #[test]
    fn test_pipeline_delay() {
        // Device A delay 0ms, Device B delay 10ms (480 frames)
        let mut pipeline =
            AudioPipeline::new(ChannelMode::Duplicate, 0, 10, 1.0, 1.0, false, false);
        let frames = generate_test_stereo_frames(5000);
        let bytes = frames_to_bytes(&frames);
        pipeline.push_raw_bytes(&bytes, frames.len());

        let mut out_a = vec![0u8; 8 * 1];
        let mut out_b = vec![0u8; 8 * 1];
        pipeline.read_frames_a(&mut out_a, 1);
        pipeline.read_frames_b(&mut out_b, 1);

        let sample_a = f32::from_ne_bytes(out_a[0..4].try_into().unwrap());
        let sample_b = f32::from_ne_bytes(out_b[0..4].try_into().unwrap());

        let expected_diff = 480.0 * 0.0001;
        assert!(
            (sample_a - sample_b - expected_diff).abs() < 1e-4,
            "sample_a={}, sample_b={}, diff={}, expected={}",
            sample_a,
            sample_b,
            sample_a - sample_b,
            expected_diff
        );
    }

    #[test]
    fn test_pipeline_intermittent_speech() {
        let mut pipeline = AudioPipeline::new(ChannelMode::Duplicate, 0, 0, 1.0, 1.0, false, false);

        // First syllable: 3000 frames
        let frames1 = generate_test_stereo_frames(3000);
        pipeline.push_raw_bytes(&frames_to_bytes(&frames1), frames1.len());

        let mut out = vec![0u8; 8 * 1000];
        pipeline.read_frames_a(&mut out, 1000);
        let sample1 = f32::from_ne_bytes(out[0..4].try_into().unwrap());
        assert!(sample1 > 0.0);

        // Read remaining frames
        pipeline.read_frames_a(&mut out, 1000);
        pipeline.read_frames_a(&mut out, 1000);

        // Gap / pause between syllables (reads while no incoming audio)
        for _ in 0..3 {
            pipeline.read_frames_a(&mut out, 1000);
            let s = f32::from_ne_bytes(out[0..4].try_into().unwrap());
            assert_eq!(s, 0.0);
        }

        // Second syllable: 3000 frames
        let frames2 = generate_test_stereo_frames(3000);
        pipeline.push_raw_bytes(&frames_to_bytes(&frames2), frames2.len());

        // Next read should play second syllable cleanly without glitches
        pipeline.read_frames_a(&mut out, 1000);
        let sample2 = f32::from_ne_bytes(out[0..4].try_into().unwrap());
        assert!(sample2 > 0.0);
    }
}
