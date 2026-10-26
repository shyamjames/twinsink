use anyhow::{Context, Result};
use pipewire as pw;
use pw::node::Node;
use pw::types::ObjectType;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::sync::{mpsc as tokio_mpsc, oneshot};
use tracing::{info, warn};
use twinsink_core::{AppConfig, AudioDevice, ChannelMode, DaemonStatus, DeviceState};

use crate::pipeline::AudioPipeline;

#[derive(Debug)]
pub enum PwCommand {
    ListDevices(oneshot::Sender<Vec<AudioDevice>>),
    ApplyConfig {
        config: AppConfig,
        resp: oneshot::Sender<Result<()>>,
    },
    SelectPair {
        device_a: String,
        device_b: String,
        resp: oneshot::Sender<Result<()>>,
    },
    GetStatus(oneshot::Sender<DaemonStatus>),
    SetVolume {
        device: String,
        volume: f32,
        resp: oneshot::Sender<Result<()>>,
    },
    SetMute {
        device: String,
        mute: Option<bool>,
        resp: oneshot::Sender<Result<()>>,
    },
    SetDelay {
        device: String,
        delay_ms: u32,
        resp: oneshot::Sender<Result<()>>,
    },
    SetMode {
        mode: ChannelMode,
        resp: oneshot::Sender<Result<()>>,
    },
    RestoreDefault(oneshot::Sender<Result<()>>),
    Terminate,
}

#[derive(Debug, Clone)]
pub enum PwEvent {
    Added(AudioDevice),
    Removed { id: u32, name: String },
    Changed(AudioDevice),
    Status(DaemonStatus),
}

struct SinkRecord {
    id: u32,
    name: String,
    description: String,
    state: DeviceState,
    _node: Node,
    _listener: pw::node::NodeListener,
}

struct RouterState {
    core: pw::core::CoreRc,
    metadata: Option<pw::metadata::Metadata>,
    _metadata_listener: Option<pw::metadata::MetadataListener>,
    virtual_node: Option<pw::node::Node>,
    virtual_sink_active: bool,
    sinks: HashMap<u32, SinkRecord>,
    selected_pair: (Option<String>, Option<String>),
    volume_a: f32,
    volume_b: f32,
    mute_a: bool,
    mute_b: bool,
    delay_ms_a: u32,
    delay_ms_b: u32,
    mode: ChannelMode,
    connected_a: bool,
    connected_b: bool,
    pipeline: Arc<Mutex<AudioPipeline>>,
    cap_stream: Option<pw::stream::StreamRc>,
    _cap_listener: Option<pw::stream::StreamListener<()>>,
    out_stream_a: Option<pw::stream::StreamRc>,
    _out_listener_a: Option<pw::stream::StreamListener<()>>,
    out_stream_b: Option<pw::stream::StreamRc>,
    _out_listener_b: Option<pw::stream::StreamListener<()>>,
    original_default_sink: Option<String>,
    configured_default_sink: Option<String>,
    event_tx: tokio_mpsc::UnboundedSender<PwEvent>,
}

pub struct PipewireHandle {
    cmd_tx: pw::channel::Sender<PwCommand>,
    join_handle: Option<thread::JoinHandle<Result<()>>>,
}

impl PipewireHandle {
    pub fn spawn(
        event_tx: tokio_mpsc::UnboundedSender<PwEvent>,
    ) -> Result<(Self, pw::channel::Sender<PwCommand>)> {
        let (cmd_tx, cmd_rx) = pw::channel::channel();
        let cmd_tx_clone = cmd_tx.clone();
        let (ready_tx, ready_rx) = std_mpsc::channel();

        let join_handle = thread::Builder::new()
            .name("pipewire-loop".to_string())
            .spawn(move || run_pipewire_loop(cmd_rx, event_tx, ready_tx))
            .context("Failed to spawn pipewire thread")?;

        ready_rx
            .recv()
            .context("PipeWire thread failed to initialize")??;

        Ok((
            Self {
                cmd_tx,
                join_handle: Some(join_handle),
            },
            cmd_tx_clone,
        ))
    }

    pub async fn list_devices(&self) -> Result<Vec<AudioDevice>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(PwCommand::ListDevices(resp_tx))
            .map_err(|_| anyhow::anyhow!("Failed to send ListDevices command"))?;
        resp_rx.await.context("Failed to receive devices")
    }

    pub async fn apply_config(&self, config: AppConfig) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(PwCommand::ApplyConfig {
                config,
                resp: resp_tx,
            })
            .map_err(|_| anyhow::anyhow!("Failed to send ApplyConfig command"))?;
        resp_rx
            .await
            .context("Failed to receive ApplyConfig reply")?
    }

    pub async fn select_pair(&self, device_a: String, device_b: String) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(PwCommand::SelectPair {
                device_a,
                device_b,
                resp: resp_tx,
            })
            .map_err(|_| anyhow::anyhow!("Failed to send SelectPair command"))?;
        resp_rx
            .await
            .context("Failed to receive select pair reply")?
    }

    pub async fn get_status(&self) -> Result<DaemonStatus> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(PwCommand::GetStatus(resp_tx))
            .map_err(|_| anyhow::anyhow!("Failed to send GetStatus command"))?;
        resp_rx.await.context("Failed to receive status")
    }

    pub async fn set_volume(&self, device: String, volume: f32) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(PwCommand::SetVolume {
                device,
                volume,
                resp: resp_tx,
            })
            .map_err(|_| anyhow::anyhow!("Failed to send SetVolume command"))?;
        resp_rx.await.context("Failed to receive SetVolume reply")?
    }

    pub async fn set_mute(&self, device: String, mute: Option<bool>) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(PwCommand::SetMute {
                device,
                mute,
                resp: resp_tx,
            })
            .map_err(|_| anyhow::anyhow!("Failed to send SetMute command"))?;
        resp_rx.await.context("Failed to receive SetMute reply")?
    }

    pub async fn set_delay(&self, device: String, delay_ms: u32) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(PwCommand::SetDelay {
                device,
                delay_ms,
                resp: resp_tx,
            })
            .map_err(|_| anyhow::anyhow!("Failed to send SetDelay command"))?;
        resp_rx.await.context("Failed to receive SetDelay reply")?
    }

    pub async fn set_mode(&self, mode: ChannelMode) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(PwCommand::SetMode {
                mode,
                resp: resp_tx,
            })
            .map_err(|_| anyhow::anyhow!("Failed to send SetMode command"))?;
        resp_rx.await.context("Failed to receive SetMode reply")?
    }

    pub async fn restore_default(&self) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(PwCommand::RestoreDefault(resp_tx))
            .map_err(|_| anyhow::anyhow!("Failed to send RestoreDefault command"))?;
        resp_rx.await.context("Failed to receive restore reply")?
    }

    pub fn shutdown(mut self) -> Result<()> {
        let _ = self.cmd_tx.send(PwCommand::Terminate);
        if let Some(handle) = self.join_handle.take() {
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("Pipewire thread panicked"))??;
        }
        Ok(())
    }
}

impl Drop for PipewireHandle {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(PwCommand::Terminate);
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
    }
}

fn get_config_dir() -> std::path::PathBuf {
    if let Ok(dir) = std::env::var("XDG_CONFIG_HOME") {
        std::path::PathBuf::from(dir).join("twinsink")
    } else if let Ok(home) = std::env::var("HOME") {
        std::path::PathBuf::from(home)
            .join(".config")
            .join("twinsink")
    } else {
        std::path::PathBuf::from("/tmp/twinsink")
    }
}

fn read_saved_default() -> Option<String> {
    let path = get_config_dir().join("saved_default_sink.txt");
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && s != "twinsink")
}

fn save_default(name: &str) {
    let dir = get_config_dir();
    let _ = std::fs::create_dir_all(&dir);
    let _ = std::fs::write(dir.join("saved_default_sink.txt"), name);
}

fn clear_saved_default() {
    let path = get_config_dir().join("saved_default_sink.txt");
    let _ = std::fs::remove_file(path);
}

pub fn apply_device_volume(device_name: &str, node_id: Option<u32>, volume: f32) {
    let vol_clamped = volume.clamp(0.0, 1.0);
    let mut success = false;
    if let Some(id) = node_id {
        if let Ok(mut child) = std::process::Command::new("wpctl")
            .args([
                "set-volume",
                &id.to_string(),
                &format!("{:.2}", vol_clamped),
            ])
            .spawn()
        {
            if child.wait().is_ok() {
                success = true;
            }
        }
    }
    if !success {
        let percent = (vol_clamped * 100.0).round() as u32;
        let _ = std::process::Command::new("pactl")
            .args(["set-sink-volume", device_name, &format!("{}%", percent)])
            .status();
    }
}

pub fn apply_device_mute(device_name: &str, node_id: Option<u32>, mute: bool) {
    let mut success = false;
    if let Some(id) = node_id {
        let mute_arg = if mute { "1" } else { "0" };
        if let Ok(mut child) = std::process::Command::new("wpctl")
            .args(["set-mute", &id.to_string(), mute_arg])
            .spawn()
        {
            if child.wait().is_ok() {
                success = true;
            }
        }
    }
    if !success {
        let mute_arg = if mute { "1" } else { "0" };
        let _ = std::process::Command::new("pactl")
            .args(["set-sink-mute", device_name, mute_arg])
            .status();
    }
}

fn current_config(state: &RouterState) -> AppConfig {
    AppConfig {
        device_a: state.selected_pair.0.clone(),
        device_b: state.selected_pair.1.clone(),
        volume_a: state.volume_a,
        volume_b: state.volume_b,
        mute_a: state.mute_a,
        mute_b: state.mute_b,
        delay_ms_a: state.delay_ms_a,
        delay_ms_b: state.delay_ms_b,
        mode: state.mode,
    }
}

fn persist_state(state: &RouterState) {
    let cfg = current_config(state);
    if let Err(e) = crate::config::save_config(&cfg) {
        warn!("Failed to persist config to TOML: {:?}", e);
    }
}

fn set_default_sink_metadata(metadata: &pw::metadata::Metadata, sink_name: &str) {
    let json_val = format!("{{\"name\":\"{}\"}}", sink_name);
    metadata.set_property(
        0,
        "default.configured.audio.sink",
        Some("Spa:String:JSON"),
        Some(&json_val),
    );
    metadata.set_property(
        0,
        "default.audio.sink",
        Some("Spa:String:JSON"),
        Some(&json_val),
    );
}

fn current_status(state: &RouterState) -> DaemonStatus {
    DaemonStatus {
        virtual_sink: "twinsink".to_string(),
        device_a: state.selected_pair.0.clone(),
        device_b: state.selected_pair.1.clone(),
        connected_a: state.connected_a,
        connected_b: state.connected_b,
        volume_a: state.volume_a,
        volume_b: state.volume_b,
        mute_a: state.mute_a,
        mute_b: state.mute_b,
        delay_ms_a: state.delay_ms_a,
        delay_ms_b: state.delay_ms_b,
        mode: state.mode,
    }
}

fn emit_status(state: &RouterState) {
    let status = current_status(state);
    let _ = state.event_tx.send(PwEvent::Status(status));
}

fn ensure_virtual_sink(state: &mut RouterState) -> Result<()> {
    if state.virtual_node.is_some() {
        return Ok(());
    }

    let node_props = pw::properties::properties! {
        "factory.name" => "support.null-audio-sink",
        "node.name" => "twinsink",
        "node.description" => "TwinSink",
        "media.class" => "Audio/Sink",
        "audio.position" => "[ FL, FR ]",
        "object.linger" => "false",
    };

    let node: pw::node::Node = state.core.create_object("adapter", &node_props)?;
    state.virtual_node = Some(node);
    state.virtual_sink_active = true;
    info!("Created twinsink virtual audio sink node");

    if let Some(ref md) = state.metadata {
        set_default_sink_metadata(md, "twinsink");
        info!("Set twinsink as system default audio sink via metadata");
    }

    Ok(())
}

fn ensure_capture_stream(state: &mut RouterState) -> Result<()> {
    if state.cap_stream.is_some() {
        return Ok(());
    }

    let cap_props = pw::properties::properties! {
        *pw::keys::MEDIA_TYPE => "Audio",
        *pw::keys::MEDIA_CATEGORY => "Capture",
        *pw::keys::MEDIA_ROLE => "Music",
        *pw::keys::NODE_NAME => "twinsink-capture",
        *pw::keys::NODE_DESCRIPTION => "TwinSink Monitor Capture",
        *pw::keys::STREAM_CAPTURE_SINK => "true",
        "target.object" => "twinsink",
        "audio.channels" => "2",
        "audio.rate" => "48000",
    };

    let stream = pw::stream::StreamRc::new(state.core.clone(), "twinsink-capture", cap_props)?;
    let pipeline = state.pipeline.clone();

    let listener = stream
        .add_local_listener::<()>()
        .process(move |stream, _| {
            if let Some(mut buf) = stream.dequeue_buffer() {
                let datas = buf.datas_mut();
                if datas.is_empty() {
                    return;
                }
                let chunk = &mut datas[0];
                let chunk_size = chunk.chunk().size() as usize;
                if let Some(bytes) = chunk.data() {
                    let n_samples = chunk_size / std::mem::size_of::<f32>();
                    if n_samples > 0 {
                        if let Ok(mut pipe) = pipeline.lock() {
                            pipe.push_raw_bytes(bytes, n_samples);
                        }
                    }
                }
            }
        })
        .register()?;

    let param_bytes = crate::pipeline::make_format_params();
    let mut params = [pw::spa::pod::Pod::from_bytes(&param_bytes).unwrap()];

    stream.connect(
        pw::spa::utils::Direction::Input,
        None,
        pw::stream::StreamFlags::AUTOCONNECT
            | pw::stream::StreamFlags::MAP_BUFFERS
            | pw::stream::StreamFlags::RT_PROCESS,
        &mut params,
    )?;

    state.cap_stream = Some(stream);
    state._cap_listener = Some(listener);
    info!("Initialized twinsink monitor capture stream");
    Ok(())
}

fn create_out_stream_a(state: &mut RouterState, target_device: &str) -> Result<()> {
    let out_props = pw::properties::properties! {
        *pw::keys::MEDIA_TYPE => "Audio",
        *pw::keys::MEDIA_CATEGORY => "Playback",
        *pw::keys::MEDIA_ROLE => "Music",
        *pw::keys::NODE_NAME => "twinsink-out-a",
        *pw::keys::NODE_DESCRIPTION => "TwinSink Output A",
        "target.object" => target_device,
        "audio.channels" => "2",
        "audio.rate" => "48000",
    };

    let stream = pw::stream::StreamRc::new(state.core.clone(), "twinsink-out-a", out_props)?;
    let pipeline = state.pipeline.clone();

    let listener = stream
        .add_local_listener::<()>()
        .process(move |stream, _| {
            if let Some(mut buf) = stream.dequeue_buffer() {
                let datas = buf.datas_mut();
                if datas.is_empty() {
                    return;
                }
                let data = &mut datas[0];
                let stride = 8;
                if let Some(slice) = data.data() {
                    let n_frames = slice.len() / stride;
                    if let Ok(mut pipe) = pipeline.lock() {
                        pipe.read_frames_a(slice, n_frames);
                    }
                    let chunk = data.chunk_mut();
                    *chunk.offset_mut() = 0;
                    *chunk.stride_mut() = stride as _;
                    *chunk.size_mut() = (stride * n_frames) as _;
                }
            }
        })
        .register()?;

    let param_bytes = crate::pipeline::make_format_params();
    let mut params = [pw::spa::pod::Pod::from_bytes(&param_bytes).unwrap()];

    stream.connect(
        pw::spa::utils::Direction::Output,
        None,
        pw::stream::StreamFlags::AUTOCONNECT
            | pw::stream::StreamFlags::MAP_BUFFERS
            | pw::stream::StreamFlags::RT_PROCESS,
        &mut params,
    )?;

    state.out_stream_a = Some(stream);
    state._out_listener_a = Some(listener);
    info!(target = target_device, "Started twinsink playback stream A");
    Ok(())
}

fn create_out_stream_b(state: &mut RouterState, target_device: &str) -> Result<()> {
    let out_props = pw::properties::properties! {
        *pw::keys::MEDIA_TYPE => "Audio",
        *pw::keys::MEDIA_CATEGORY => "Playback",
        *pw::keys::MEDIA_ROLE => "Music",
        *pw::keys::NODE_NAME => "twinsink-out-b",
        *pw::keys::NODE_DESCRIPTION => "TwinSink Output B",
        "target.object" => target_device,
        "audio.channels" => "2",
        "audio.rate" => "48000",
    };

    let stream = pw::stream::StreamRc::new(state.core.clone(), "twinsink-out-b", out_props)?;
    let pipeline = state.pipeline.clone();

    let listener = stream
        .add_local_listener::<()>()
        .process(move |stream, _| {
            if let Some(mut buf) = stream.dequeue_buffer() {
                let datas = buf.datas_mut();
                if datas.is_empty() {
                    return;
                }
                let data = &mut datas[0];
                let stride = 8;
                if let Some(slice) = data.data() {
                    let n_frames = slice.len() / stride;
                    if let Ok(mut pipe) = pipeline.lock() {
                        pipe.read_frames_b(slice, n_frames);
                    }
                    let chunk = data.chunk_mut();
                    *chunk.offset_mut() = 0;
                    *chunk.stride_mut() = stride as _;
                    *chunk.size_mut() = (stride * n_frames) as _;
                }
            }
        })
        .register()?;

    let param_bytes = crate::pipeline::make_format_params();
    let mut params = [pw::spa::pod::Pod::from_bytes(&param_bytes).unwrap()];

    stream.connect(
        pw::spa::utils::Direction::Output,
        None,
        pw::stream::StreamFlags::AUTOCONNECT
            | pw::stream::StreamFlags::MAP_BUFFERS
            | pw::stream::StreamFlags::RT_PROCESS,
        &mut params,
    )?;

    state.out_stream_b = Some(stream);
    state._out_listener_b = Some(listener);
    info!(target = target_device, "Started twinsink playback stream B");
    Ok(())
}

fn update_output_streams(state: &mut RouterState) {
    // Stream A
    if let Some(ref target_a) = state.selected_pair.0.clone() {
        if state.sinks.values().any(|s| &s.name == target_a) {
            if state.out_stream_a.is_none() {
                if let Err(e) = create_out_stream_a(state, target_a) {
                    warn!(target = %target_a, "Failed to create output stream A: {:?}", e);
                    state.connected_a = false;
                } else {
                    state.connected_a = true;
                }
            } else {
                state.connected_a = true;
            }
        } else {
            state._out_listener_a = None;
            state.out_stream_a = None;
            state.connected_a = false;
        }
    } else {
        state._out_listener_a = None;
        state.out_stream_a = None;
        state.connected_a = false;
    }

    // Stream B
    if let Some(ref target_b) = state.selected_pair.1.clone() {
        if state.sinks.values().any(|s| &s.name == target_b) {
            if state.out_stream_b.is_none() {
                if let Err(e) = create_out_stream_b(state, target_b) {
                    warn!(target = %target_b, "Failed to create output stream B: {:?}", e);
                    state.connected_b = false;
                } else {
                    state.connected_b = true;
                }
            } else {
                state.connected_b = true;
            }
        } else {
            state._out_listener_b = None;
            state.out_stream_b = None;
            state.connected_b = false;
        }
    } else {
        state._out_listener_b = None;
        state.out_stream_b = None;
        state.connected_b = false;
    }
}

fn shutdown_router(state: &mut RouterState) {
    info!("Shutting down twinsink audio routing");
    if let Some(ref orig) = state.original_default_sink {
        if let Some(ref md) = state.metadata {
            info!(restored = %orig, "Restoring original default sink on shutdown");
            set_default_sink_metadata(md, orig);
        }
    }
    clear_saved_default();
    state._out_listener_a = None;
    state.out_stream_a = None;
    state._out_listener_b = None;
    state.out_stream_b = None;
    state._cap_listener = None;
    state.cap_stream = None;
    state.virtual_node = None;
    state.virtual_sink_active = false;
    state.connected_a = false;
    state.connected_b = false;
}

fn run_pipewire_loop(
    cmd_rx: pw::channel::Receiver<PwCommand>,
    event_tx: tokio_mpsc::UnboundedSender<PwEvent>,
    ready_tx: std_mpsc::Sender<Result<()>>,
) -> Result<()> {
    pw::init();

    let main_loop = match pw::main_loop::MainLoopRc::new(None) {
        Ok(l) => l,
        Err(e) => {
            let err = anyhow::anyhow!("Failed to create pipewire main loop: {:?}", e);
            let _ = ready_tx.send(Err(err));
            return Err(anyhow::anyhow!("{:?}", e));
        }
    };

    let context = match pw::context::ContextRc::new(&main_loop, None) {
        Ok(c) => c,
        Err(e) => {
            let err = anyhow::anyhow!("Failed to create pipewire context: {:?}", e);
            let _ = ready_tx.send(Err(err));
            return Err(anyhow::anyhow!("{:?}", e));
        }
    };

    let core = match context.connect_rc(None) {
        Ok(c) => c,
        Err(e) => {
            let err = anyhow::anyhow!("Failed to connect to pipewire core: {:?}", e);
            let _ = ready_tx.send(Err(err));
            return Err(anyhow::anyhow!("{:?}", e));
        }
    };

    let registry = match core.get_registry_rc() {
        Ok(r) => r,
        Err(e) => {
            let err = anyhow::anyhow!("Failed to get pipewire registry: {:?}", e);
            let _ = ready_tx.send(Err(err));
            return Err(anyhow::anyhow!("{:?}", e));
        }
    };

    let pipeline = Arc::new(Mutex::new(AudioPipeline::new(
        ChannelMode::Duplicate,
        0,
        0,
        1.0,
        1.0,
        false,
        false,
    )));

    let state = Rc::new(RefCell::new(RouterState {
        core: core.clone(),
        metadata: None,
        _metadata_listener: None,
        virtual_node: None,
        virtual_sink_active: false,
        sinks: HashMap::new(),
        selected_pair: (None, None),
        volume_a: 1.0,
        volume_b: 1.0,
        mute_a: false,
        mute_b: false,
        delay_ms_a: 0,
        delay_ms_b: 0,
        mode: ChannelMode::Duplicate,
        connected_a: false,
        connected_b: false,
        pipeline,
        cap_stream: None,
        _cap_listener: None,
        out_stream_a: None,
        _out_listener_a: None,
        out_stream_b: None,
        _out_listener_b: None,
        original_default_sink: None,
        configured_default_sink: None,
        event_tx: event_tx.clone(),
    }));

    let loop_clone = main_loop.clone();
    let state_for_cmds = state.clone();

    let _cmd_attached = cmd_rx.attach(main_loop.loop_(), move |cmd| match cmd {
        PwCommand::ListDevices(resp_tx) => {
            let mut devices: Vec<AudioDevice> = state_for_cmds
                .borrow()
                .sinks
                .values()
                .map(|r| AudioDevice {
                    id: r.id,
                    name: r.name.clone(),
                    description: r.description.clone(),
                    state: r.state,
                    volume: 1.0,
                    mute: false,
                })
                .collect();
            devices.sort_by(|a, b| a.name.cmp(&b.name));
            let _ = resp_tx.send(devices);
        }
        PwCommand::ApplyConfig { config, resp } => {
            let mut st = state_for_cmds.borrow_mut();
            st.selected_pair = (config.device_a.clone(), config.device_b.clone());
            st.volume_a = config.volume_a;
            st.volume_b = config.volume_b;
            st.mute_a = config.mute_a;
            st.mute_b = config.mute_b;
            st.delay_ms_a = config.delay_ms_a;
            st.delay_ms_b = config.delay_ms_b;
            st.mode = config.mode;

            if let Ok(mut pipe) = st.pipeline.lock() {
                pipe.volume_a = config.volume_a;
                pipe.volume_b = config.volume_b;
                pipe.mute_a = config.mute_a;
                pipe.mute_b = config.mute_b;
                pipe.set_delay_a(config.delay_ms_a);
                pipe.set_delay_b(config.delay_ms_b);
                pipe.set_mode(config.mode);
            }

            if config.device_a.is_some() || config.device_b.is_some() {
                if let Err(e) = ensure_virtual_sink(&mut st) {
                    let _ = resp.send(Err(e));
                    return;
                }
                if let Err(e) = ensure_capture_stream(&mut st) {
                    let _ = resp.send(Err(e));
                    return;
                }
                update_output_streams(&mut st);
            }
            emit_status(&st);
            persist_state(&st);
            let _ = resp.send(Ok(()));
        }
        PwCommand::SelectPair {
            device_a,
            device_b,
            resp,
        } => {
            if device_a == device_b {
                let _ = resp.send(Err(anyhow::anyhow!(
                    "Device A and Device B must be different outputs"
                )));
                return;
            }

            let mut st = state_for_cmds.borrow_mut();
            st.selected_pair = (Some(device_a.clone()), Some(device_b.clone()));

            // Reset previous output streams
            st._out_listener_a = None;
            st.out_stream_a = None;
            st._out_listener_b = None;
            st.out_stream_b = None;
            st.connected_a = false;
            st.connected_b = false;

            if let Err(e) = ensure_virtual_sink(&mut st) {
                let _ = resp.send(Err(e));
                return;
            }
            if let Err(e) = ensure_capture_stream(&mut st) {
                let _ = resp.send(Err(e));
                return;
            }

            update_output_streams(&mut st);
            emit_status(&st);
            persist_state(&st);
            let _ = resp.send(Ok(()));
        }
        PwCommand::GetStatus(resp_tx) => {
            let status = current_status(&state_for_cmds.borrow());
            let _ = resp_tx.send(status);
        }
        PwCommand::SetVolume {
            device,
            volume,
            resp,
        } => {
            let mut st = state_for_cmds.borrow_mut();
            let vol = volume.clamp(0.0, 1.0);
            let mut found = false;
            if st.selected_pair.0.as_deref() == Some(&device) {
                st.volume_a = vol;
                if let Ok(mut pipe) = st.pipeline.lock() {
                    pipe.set_volume_a(vol);
                }
                let node_id = st.sinks.values().find(|s| s.name == device).map(|s| s.id);
                apply_device_volume(&device, node_id, vol);
                found = true;
            }
            if st.selected_pair.1.as_deref() == Some(&device) {
                st.volume_b = vol;
                if let Ok(mut pipe) = st.pipeline.lock() {
                    pipe.set_volume_b(vol);
                }
                let node_id = st.sinks.values().find(|s| s.name == device).map(|s| s.id);
                apply_device_volume(&device, node_id, vol);
                found = true;
            }
            if found {
                emit_status(&st);
                persist_state(&st);
                let _ = resp.send(Ok(()));
            } else {
                let _ = resp.send(Err(anyhow::anyhow!(
                    "Device '{}' is not one of the selected outputs",
                    device
                )));
            }
        }
        PwCommand::SetMute { device, mute, resp } => {
            let mut st = state_for_cmds.borrow_mut();
            let mut found = false;
            if st.selected_pair.0.as_deref() == Some(&device) {
                let new_mute = mute.unwrap_or(!st.mute_a);
                st.mute_a = new_mute;
                if let Ok(mut pipe) = st.pipeline.lock() {
                    pipe.set_mute_a(new_mute);
                }
                let node_id = st.sinks.values().find(|s| s.name == device).map(|s| s.id);
                apply_device_mute(&device, node_id, new_mute);
                found = true;
            }
            if st.selected_pair.1.as_deref() == Some(&device) {
                let new_mute = mute.unwrap_or(!st.mute_b);
                st.mute_b = new_mute;
                if let Ok(mut pipe) = st.pipeline.lock() {
                    pipe.set_mute_b(new_mute);
                }
                let node_id = st.sinks.values().find(|s| s.name == device).map(|s| s.id);
                apply_device_mute(&device, node_id, new_mute);
                found = true;
            }
            if found {
                emit_status(&st);
                persist_state(&st);
                let _ = resp.send(Ok(()));
            } else {
                let _ = resp.send(Err(anyhow::anyhow!(
                    "Device '{}' is not one of the selected outputs",
                    device
                )));
            }
        }
        PwCommand::SetDelay {
            device,
            delay_ms,
            resp,
        } => {
            let mut st = state_for_cmds.borrow_mut();
            let mut found = false;
            if st.selected_pair.0.as_deref() == Some(&device) {
                st.delay_ms_a = delay_ms;
                if let Ok(mut pipe) = st.pipeline.lock() {
                    pipe.set_delay_a(delay_ms);
                }
                found = true;
            }
            if st.selected_pair.1.as_deref() == Some(&device) {
                st.delay_ms_b = delay_ms;
                if let Ok(mut pipe) = st.pipeline.lock() {
                    pipe.set_delay_b(delay_ms);
                }
                found = true;
            }
            if found {
                info!(device = %device, delay_ms = delay_ms, "Updated output delay");
                emit_status(&st);
                persist_state(&st);
                let _ = resp.send(Ok(()));
            } else {
                let _ = resp.send(Err(anyhow::anyhow!(
                    "Device '{}' is not one of the selected outputs",
                    device
                )));
            }
        }
        PwCommand::SetMode { mode, resp } => {
            let mut st = state_for_cmds.borrow_mut();
            st.mode = mode;
            if let Ok(mut pipe) = st.pipeline.lock() {
                pipe.set_mode(mode);
            }
            info!(mode = %mode, "Updated router channel mode");
            emit_status(&st);
            persist_state(&st);
            let _ = resp.send(Ok(()));
        }
        PwCommand::RestoreDefault(resp_tx) => {
            let st = state_for_cmds.borrow();
            if let Some(ref orig) = st.original_default_sink.clone() {
                if let Some(ref md) = st.metadata {
                    set_default_sink_metadata(md, orig);
                }
            }
            let _ = resp_tx.send(Ok(()));
        }
        PwCommand::Terminate => {
            let mut st = state_for_cmds.borrow_mut();
            shutdown_router(&mut st);
            loop_clone.quit();
        }
    });

    let registry_weak = registry.downgrade();
    let state_for_reg = state.clone();

    let _reg_listener = registry
        .add_listener_local()
        .global(move |obj| {
            let props = match obj.props {
                Some(p) => p,
                None => return,
            };

            let registry = match registry_weak.upgrade() {
                Some(r) => r,
                None => return,
            };

            if obj.type_ == ObjectType::Node {
                let name = match props.get(*pw::keys::NODE_NAME) {
                    Some(n) => n.to_string(),
                    None => return,
                };

                if name == "twinsink" || name.starts_with("twinsink-") {
                    return;
                }

                let media_class = props.get(*pw::keys::MEDIA_CLASS);
                if media_class != Some("Audio/Sink") {
                    return;
                }

                let description = props
                    .get(*pw::keys::NODE_DESCRIPTION)
                    .or_else(|| props.get(*pw::keys::NODE_NICK))
                    .unwrap_or(&name)
                    .to_string();

                let id = obj.id;
                let node: Node = match registry.bind(obj) {
                    Ok(n) => n,
                    Err(e) => {
                        warn!(id = id, name = %name, error = ?e, "Failed to bind sink node");
                        return;
                    }
                };

                let state_info = state_for_reg.clone();
                let name_info = name.clone();
                let desc_info = description.clone();

                let listener = node
                    .add_listener_local()
                    .info(move |info| {
                        let node_state = match info.state() {
                            pw::node::NodeState::Running => DeviceState::Active,
                            pw::node::NodeState::Idle => DeviceState::Idle,
                            pw::node::NodeState::Suspended => DeviceState::Suspended,
                            _ => DeviceState::Unknown,
                        };

                        let mut st = state_info.borrow_mut();
                        if let Some(entry) = st.sinks.get_mut(&id) {
                            if entry.state != node_state {
                                entry.state = node_state;
                                info!(
                                    id = id,
                                    name = %name_info,
                                    state = ?node_state,
                                    "Audio sink state changed"
                                );
                                let _ = st.event_tx.send(PwEvent::Changed(AudioDevice {
                                    id,
                                    name: name_info.clone(),
                                    description: desc_info.clone(),
                                    state: node_state,
                                    volume: 1.0,
                                    mute: false,
                                }));
                            }
                        }
                    })
                    .register();

                info!(
                    id = id,
                    name = %name,
                    description = %description,
                    "Audio sink discovered"
                );

                let device = AudioDevice {
                    id,
                    name: name.clone(),
                    description: description.clone(),
                    state: DeviceState::Unknown,
                    volume: 1.0,
                    mute: false,
                };

                let mut st = state_for_reg.borrow_mut();
                let _ = st.event_tx.send(PwEvent::Added(device));

                st.sinks.insert(
                    id,
                    SinkRecord {
                        id,
                        name: name.clone(),
                        description,
                        state: DeviceState::Unknown,
                        _node: node,
                        _listener: listener,
                    },
                );

                // Auto re-connect output streams if matching selected pair
                update_output_streams(&mut st);
                emit_status(&st);
            } else if obj.type_ == ObjectType::Metadata {
                let metadata_name = props.get("metadata.name");
                if metadata_name == Some("default") {
                    let metadata: pw::metadata::Metadata = match registry.bind(obj) {
                        Ok(m) => m,
                        Err(e) => {
                            warn!("Failed to bind default metadata: {:?}", e);
                            return;
                        }
                    };

                    let state_meta = state_for_reg.clone();
                    let listener = metadata
                        .add_listener_local()
                        .property(move |subject, key, _type, value| {
                            if subject == 0 && key == Some("default.audio.sink") {
                                if let Some(val) = value {
                                    if let Ok(json) =
                                        serde_json::from_str::<serde_json::Value>(val)
                                    {
                                        if let Some(sink_name) =
                                            json.get("name").and_then(|n| n.as_str())
                                        {
                                            let mut st = state_meta.borrow_mut();
                                            if sink_name == "twinsink" {
                                                if !st.virtual_sink_active {
                                                    // Stale twinsink detected from a crashed run!
                                                    let fallback = read_saved_default()
                                                        .or_else(|| {
                                                            st.configured_default_sink.clone()
                                                        })
                                                        .or_else(|| {
                                                            st.sinks
                                                                 .values()
                                                                 .map(|s| s.name.clone())
                                                                 .next()
                                                        });
                                                    if let Some(fb) = fallback {
                                                        warn!(
                                                            restored = %fb,
                                                            "Detected stale twinsink as default from crashed run; restoring sane default"
                                                        );
                                                        if let Some(ref md) = st.metadata {
                                                            set_default_sink_metadata(md, &fb);
                                                        }
                                                        st.original_default_sink =
                                                            Some(fb.clone());
                                                        save_default(&fb);
                                                    }
                                                }
                                            } else if !st.virtual_sink_active
                                                && st.original_default_sink.is_none()
                                            {
                                                st.original_default_sink =
                                                    Some(sink_name.to_string());
                                                save_default(sink_name);
                                                info!(
                                                    sink = sink_name,
                                                    "Recorded original default sink"
                                                );
                                            }
                                        }
                                    }
                                }
                            } else if subject == 0
                                && key == Some("default.configured.audio.sink")
                            {
                                if let Some(val) = value {
                                    if let Ok(json) =
                                        serde_json::from_str::<serde_json::Value>(val)
                                    {
                                        if let Some(sink_name) =
                                            json.get("name").and_then(|n| n.as_str())
                                        {
                                            let mut st = state_meta.borrow_mut();
                                            if sink_name != "twinsink" {
                                                st.configured_default_sink =
                                                    Some(sink_name.to_string());
                                                if !st.virtual_sink_active
                                                    && st.original_default_sink.is_none()
                                                {
                                                    st.original_default_sink =
                                                        Some(sink_name.to_string());
                                                    save_default(sink_name);
                                                    info!(
                                                        sink = sink_name,
                                                        "Recorded configured default sink"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            0
                        })
                        .register();

                    let mut st = state_for_reg.borrow_mut();
                    st.metadata = Some(metadata);
                    st._metadata_listener = Some(listener);
                    info!("Bound default metadata listener");
                }
            }
        })
        .global_remove({
            let state_for_remove = state.clone();
            move |id| {
                let mut st = state_for_remove.borrow_mut();
                if let Some(removed) = st.sinks.remove(&id) {
                    info!(id = id, name = %removed.name, "Audio sink removed");
                    let _ = st.event_tx.send(PwEvent::Removed {
                        id,
                        name: removed.name.clone(),
                    });

                    let mut changed = false;
                    if st.selected_pair.0.as_deref() == Some(&removed.name) {
                        st._out_listener_a = None;
                        st.out_stream_a = None;
                        st.connected_a = false;
                        changed = true;
                    }
                    if st.selected_pair.1.as_deref() == Some(&removed.name) {
                        st._out_listener_b = None;
                        st.out_stream_b = None;
                        st.connected_b = false;
                        changed = true;
                    }
                    if changed {
                        warn!(
                            sink = %removed.name,
                            "Selected sink disconnected; output stream closed"
                        );
                        emit_status(&st);
                    }
                }
            }
        })
        .register();

    let _ = ready_tx.send(Ok(()));

    main_loop.run();

    drop(_reg_listener);
    drop(_cmd_attached);
    {
        let mut st = state.borrow_mut();
        st._out_listener_a = None;
        st.out_stream_a = None;
        st._out_listener_b = None;
        st.out_stream_b = None;
        st._cap_listener = None;
        st.cap_stream = None;
        st.sinks.clear();
        st.virtual_node = None;
        st.metadata = None;
        st._metadata_listener = None;
    }
    drop(registry);
    drop(core);
    drop(context);
    drop(main_loop);

    Ok(())
}
