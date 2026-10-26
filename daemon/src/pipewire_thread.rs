use anyhow::{Context, Result};
use pipewire as pw;
use pw::node::Node;
use pw::types::ObjectType;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::mpsc as std_mpsc;
use std::thread;
use tokio::sync::{mpsc as tokio_mpsc, oneshot};
use tracing::{info, warn};
use twinsink_core::{AudioDevice, ChannelMode, DaemonStatus, DeviceState};

#[derive(Debug)]
pub enum PwCommand {
    ListDevices(oneshot::Sender<Vec<AudioDevice>>),
    SelectPair {
        device_a: String,
        device_b: String,
        resp: oneshot::Sender<Result<()>>,
    },
    #[allow(dead_code)]
    GetStatus(oneshot::Sender<DaemonStatus>),
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

#[derive(Debug, Clone)]
struct PortRecord {
    id: u32,
    node_id: u32,
    direction: String,
    name: String,
    channel: Option<String>,
}

struct LinkedDevice {
    device_name: String,
    _link_fl: pw::link::Link,
    _link_fr: pw::link::Link,
}

struct RouterState {
    core: pw::core::CoreRc,
    metadata: Option<pw::metadata::Metadata>,
    _metadata_listener: Option<pw::metadata::MetadataListener>,
    virtual_node: Option<pw::node::Node>,
    virtual_node_id: Option<u32>,
    virtual_sink_active: bool,
    sinks: HashMap<u32, SinkRecord>,
    ports: HashMap<u32, PortRecord>,
    selected_pair: (Option<String>, Option<String>),
    links: Vec<LinkedDevice>,
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

    #[allow(dead_code)]
    pub async fn get_status(&self) -> Result<DaemonStatus> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(PwCommand::GetStatus(resp_tx))
            .map_err(|_| anyhow::anyhow!("Failed to send GetStatus command"))?;
        resp_rx.await.context("Failed to receive status")
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

fn is_fl(port: &PortRecord) -> bool {
    if let Some(ref ch) = port.channel {
        if ch == "FL" || ch.eq_ignore_ascii_case("front-left") || ch.eq_ignore_ascii_case("mono") {
            return true;
        }
    }
    let lower = port.name.to_lowercase();
    lower.contains("fl") || lower.contains("left") || lower.ends_with("_0") || lower.ends_with(":1")
}

fn is_fr(port: &PortRecord) -> bool {
    if let Some(ref ch) = port.channel {
        if ch == "FR" || ch.eq_ignore_ascii_case("front-right") {
            return true;
        }
    }
    let lower = port.name.to_lowercase();
    lower.contains("fr")
        || lower.contains("right")
        || lower.ends_with("_1")
        || lower.ends_with(":2")
}

fn current_status(state: &RouterState) -> DaemonStatus {
    let connected_a = state
        .selected_pair
        .0
        .as_ref()
        .is_some_and(|name| state.links.iter().any(|l| &l.device_name == name));
    let connected_b = state
        .selected_pair
        .1
        .as_ref()
        .is_some_and(|name| state.links.iter().any(|l| &l.device_name == name));

    DaemonStatus {
        virtual_sink: "twinsink".to_string(),
        device_a: state.selected_pair.0.clone(),
        device_b: state.selected_pair.1.clone(),
        connected_a,
        connected_b,
        volume_a: 1.0,
        volume_b: 1.0,
        mute_a: false,
        mute_b: false,
        delay_ms_a: 0,
        delay_ms_b: 0,
        mode: ChannelMode::Duplicate,
    }
}

fn emit_status(state: &RouterState) {
    let status = current_status(state);
    let _ = state.event_tx.send(PwEvent::Status(status));
}

fn try_link_device(state: &mut RouterState, target_name: &str) -> Result<bool> {
    if state.links.iter().any(|l| l.device_name == target_name) {
        return Ok(true);
    }

    let virtual_id = match state.virtual_node_id {
        Some(id) => id,
        None => return Ok(false),
    };

    let target_node_id = match state.sinks.values().find(|s| s.name == target_name) {
        Some(s) => s.id,
        None => return Ok(false),
    };

    let fl_out_id = match state
        .ports
        .values()
        .find(|p| p.node_id == virtual_id && p.direction == "out" && is_fl(p))
    {
        Some(p) => p.id,
        None => return Ok(false),
    };

    let fr_out_id = match state
        .ports
        .values()
        .find(|p| p.node_id == virtual_id && p.direction == "out" && is_fr(p))
    {
        Some(p) => p.id,
        None => return Ok(false),
    };

    let fl_in_id = match state
        .ports
        .values()
        .find(|p| p.node_id == target_node_id && p.direction == "in" && is_fl(p))
    {
        Some(p) => p.id,
        None => return Ok(false),
    };

    let fr_in_id = match state
        .ports
        .values()
        .find(|p| p.node_id == target_node_id && p.direction == "in" && is_fr(p))
    {
        Some(p) => p.id,
        None => return Ok(false),
    };

    let link_fl = state.core.create_object::<pw::link::Link>(
        "link-factory",
        &pw::properties::properties! {
            "link.output.node" => virtual_id.to_string(),
            "link.output.port" => fl_out_id.to_string(),
            "link.input.node" => target_node_id.to_string(),
            "link.input.port" => fl_in_id.to_string(),
            "object.linger" => "false",
        },
    )?;

    let link_fr = state.core.create_object::<pw::link::Link>(
        "link-factory",
        &pw::properties::properties! {
            "link.output.node" => virtual_id.to_string(),
            "link.output.port" => fr_out_id.to_string(),
            "link.input.node" => target_node_id.to_string(),
            "link.input.port" => fr_in_id.to_string(),
            "object.linger" => "false",
        },
    )?;

    info!(
        sink = target_name,
        node_id = target_node_id,
        "Successfully linked twinsink monitor ports to sink"
    );

    state.links.push(LinkedDevice {
        device_name: target_name.to_string(),
        _link_fl: link_fl,
        _link_fr: link_fr,
    });

    Ok(true)
}

fn try_relink_all(state: &mut RouterState) {
    let mut changed = false;
    if let Some(ref dev_a) = state.selected_pair.0.clone() {
        if let Ok(linked) = try_link_device(state, dev_a) {
            if linked {
                changed = true;
            }
        }
    }
    if let Some(ref dev_b) = state.selected_pair.1.clone() {
        if let Ok(linked) = try_link_device(state, dev_b) {
            if linked {
                changed = true;
            }
        }
    }
    if changed {
        emit_status(state);
    }
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

fn shutdown_router(state: &mut RouterState) {
    info!("Shutting down twinsink audio routing");
    if let Some(ref orig) = state.original_default_sink {
        if let Some(ref md) = state.metadata {
            info!(restored = %orig, "Restoring original default sink on shutdown");
            set_default_sink_metadata(md, orig);
        }
    }
    clear_saved_default();
    state.links.clear();
    state.virtual_node = None;
    state.virtual_sink_active = false;
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

    let state = Rc::new(RefCell::new(RouterState {
        core: core.clone(),
        metadata: None,
        _metadata_listener: None,
        virtual_node: None,
        virtual_node_id: None,
        virtual_sink_active: false,
        sinks: HashMap::new(),
        ports: HashMap::new(),
        selected_pair: (None, None),
        links: Vec::new(),
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
            st.links.clear();

            if let Err(e) = ensure_virtual_sink(&mut st) {
                let _ = resp.send(Err(e));
                return;
            }

            try_relink_all(&mut st);
            emit_status(&st);
            let _ = resp.send(Ok(()));
        }
        PwCommand::GetStatus(resp_tx) => {
            let status = current_status(&state_for_cmds.borrow());
            let _ = resp_tx.send(status);
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

                if name == "twinsink" {
                    info!(id = obj.id, "Discovered twinsink node in registry");
                    let mut st = state_for_reg.borrow_mut();
                    st.virtual_node_id = Some(obj.id);
                    try_relink_all(&mut st);
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

                // Auto re-link if device is in selected pair
                try_relink_all(&mut st);
            } else if obj.type_ == ObjectType::Port {
                let node_id: u32 = match props.get(*pw::keys::NODE_ID).and_then(|s| s.parse().ok())
                {
                    Some(id) => id,
                    None => return,
                };
                let direction = props.get(*pw::keys::PORT_DIRECTION).unwrap_or("").to_string();
                let name = props.get(*pw::keys::PORT_NAME).unwrap_or("").to_string();
                let channel = props.get(*pw::keys::AUDIO_CHANNEL).map(|s| s.to_string());

                let mut st = state_for_reg.borrow_mut();
                st.ports.insert(
                    obj.id,
                    PortRecord {
                        id: obj.id,
                        node_id,
                        direction,
                        name,
                        channel,
                    },
                );
                try_relink_all(&mut st);
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
                st.ports.remove(&id);
                if let Some(removed) = st.sinks.remove(&id) {
                    info!(id = id, name = %removed.name, "Audio sink removed");
                    let _ = st.event_tx.send(PwEvent::Removed {
                        id,
                        name: removed.name.clone(),
                    });
                    // Unlink device if it was linked
                    let was_linked = st.links.iter().any(|l| l.device_name == removed.name);
                    st.links.retain(|l| l.device_name != removed.name);
                    if was_linked {
                        warn!(
                            sink = %removed.name,
                            "Selected sink disconnected; removed active link"
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
        st.links.clear();
        st.sinks.clear();
        st.ports.clear();
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
