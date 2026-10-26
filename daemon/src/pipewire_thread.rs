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
use twinsink_core::{AudioDevice, DeviceState};

#[derive(Debug)]
pub enum PwCommand {
    ListDevices(oneshot::Sender<Vec<AudioDevice>>),
    Terminate,
}

#[derive(Debug, Clone)]
pub enum PwEvent {
    Added(AudioDevice),
    Removed { id: u32, name: String },
    Changed(AudioDevice),
}

struct SinkRecord {
    id: u32,
    name: String,
    description: String,
    state: DeviceState,
    _node: Node,
    _listener: pw::node::NodeListener,
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

        // Wait until PipeWire loop is initialized and running
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
            .map_err(|_| {
                anyhow::anyhow!("Failed to send ListDevices command to pipewire thread")
            })?;
        resp_rx
            .await
            .context("Failed to receive devices from pipewire thread")
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

    let sinks: Rc<RefCell<HashMap<u32, SinkRecord>>> = Rc::new(RefCell::new(HashMap::new()));
    let loop_clone = main_loop.clone();

    // Attach command receiver to mainloop
    let sinks_for_cmds = sinks.clone();
    let _cmd_attached = cmd_rx.attach(main_loop.loop_(), move |cmd| match cmd {
        PwCommand::ListDevices(resp_tx) => {
            let mut devices: Vec<AudioDevice> = sinks_for_cmds
                .borrow()
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
        PwCommand::Terminate => {
            loop_clone.quit();
        }
    });

    let registry_weak = registry.downgrade();
    let sinks_for_registry = sinks.clone();
    let event_tx_for_reg = event_tx.clone();

    let sinks_for_remove = sinks.clone();
    let _reg_listener = registry
        .add_listener_local()
        .global(move |obj| {
            if obj.type_ != ObjectType::Node {
                return;
            }

            let props = match obj.props {
                Some(p) => p,
                None => return,
            };

            let media_class = props.get(*pw::keys::MEDIA_CLASS);
            if media_class != Some("Audio/Sink") {
                return;
            }

            let name = match props.get(*pw::keys::NODE_NAME) {
                Some(n) => n.to_string(),
                None => return,
            };

            // Ignore twinsink virtual sink
            if name == "twinsink" {
                return;
            }

            let description = props
                .get(*pw::keys::NODE_DESCRIPTION)
                .or_else(|| props.get(*pw::keys::NODE_NICK))
                .unwrap_or(&name)
                .to_string();

            let id = obj.id;
            let registry = match registry_weak.upgrade() {
                Some(r) => r,
                None => return,
            };

            let node: Node = match registry.bind(obj) {
                Ok(n) => n,
                Err(e) => {
                    warn!(id = id, name = %name, error = ?e, "Failed to bind sink node");
                    return;
                }
            };

            let sinks_info = sinks_for_registry.clone();
            let event_tx_info = event_tx_for_reg.clone();
            let name_info = name.clone();
            let desc_info = description.clone();

            let listener = node
                .add_listener_local()
                .info(move |info| {
                    let state = match info.state() {
                        pw::node::NodeState::Running => DeviceState::Active,
                        pw::node::NodeState::Idle => DeviceState::Idle,
                        pw::node::NodeState::Suspended => DeviceState::Suspended,
                        _ => DeviceState::Unknown,
                    };

                    let mut sinks_ref = sinks_info.borrow_mut();
                    if let Some(entry) = sinks_ref.get_mut(&id) {
                        if entry.state != state {
                            entry.state = state;
                            info!(
                                id = id,
                                name = %name_info,
                                state = ?state,
                                "Audio sink state changed"
                            );
                            let _ = event_tx_info.send(PwEvent::Changed(AudioDevice {
                                id,
                                name: name_info.clone(),
                                description: desc_info.clone(),
                                state,
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

            let _ = event_tx_for_reg.send(PwEvent::Added(device));

            sinks_for_registry.borrow_mut().insert(
                id,
                SinkRecord {
                    id,
                    name,
                    description,
                    state: DeviceState::Unknown,
                    _node: node,
                    _listener: listener,
                },
            );
        })
        .global_remove(move |id| {
            if let Some(removed) = sinks_for_remove.borrow_mut().remove(&id) {
                info!(
                    id = id,
                    name = %removed.name,
                    "Audio sink removed"
                );
                let _ = event_tx.send(PwEvent::Removed {
                    id,
                    name: removed.name,
                });
            }
        })
        .register();

    // Signal to caller that initialization is complete
    let _ = ready_tx.send(Ok(()));

    // Run the event loop
    main_loop.run();

    // Drop sinks and listeners before exiting
    drop(_reg_listener);
    drop(_cmd_attached);
    sinks.borrow_mut().clear();
    drop(registry);
    drop(core);
    drop(context);
    drop(main_loop);

    Ok(())
}
