use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::broadcast;
use tracing::{error, info, warn};
use twinsink_core::{socket_path, DaemonEvent, IpcRequest, IpcResponse};

use crate::pipewire_thread::PipewireHandle;

pub struct IpcServer {
    listener: UnixListener,
    handle: Arc<PipewireHandle>,
    event_tx: broadcast::Sender<DaemonEvent>,
    socket_path: std::path::PathBuf,
}

impl IpcServer {
    pub async fn bind(
        handle: Arc<PipewireHandle>,
        event_tx: broadcast::Sender<DaemonEvent>,
    ) -> Result<Self> {
        let sock_path = socket_path();
        if let Some(parent) = sock_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Check if existing socket is active or stale
        if sock_path.exists() {
            match UnixStream::connect(&sock_path).await {
                Ok(_) => {
                    anyhow::bail!(
                        "Another twinsink daemon is already running (connected to {})",
                        sock_path.display()
                    );
                }
                Err(_) => {
                    info!(
                        path = %sock_path.display(),
                        "Removing stale twinsink socket file"
                    );
                    let _ = tokio::fs::remove_file(&sock_path).await;
                }
            }
        }

        let listener = UnixListener::bind(&sock_path)
            .with_context(|| format!("Failed to bind Unix socket at {}", sock_path.display()))?;

        info!(path = %sock_path.display(), "IPC server listening on Unix socket");

        Ok(Self {
            listener,
            handle,
            event_tx,
            socket_path: sock_path,
        })
    }

    pub async fn run(self) -> Result<()> {
        let handle = self.handle.clone();
        let event_tx = self.event_tx.clone();

        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => {
                    let client_handle = handle.clone();
                    let client_event_rx = event_tx.subscribe();
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(stream, client_handle, client_event_rx).await
                        {
                            warn!("IPC client error: {:?}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Error accepting IPC connection: {:?}", e);
                    break;
                }
            }
        }
        Ok(())
    }
}

impl Drop for IpcServer {
    fn drop(&mut self) {
        let path = &self.socket_path;
        if path.exists() {
            let _ = std::fs::remove_file(path);
        }
    }
}

async fn handle_client(
    stream: UnixStream,
    handle: Arc<PipewireHandle>,
    mut event_rx: broadcast::Receiver<DaemonEvent>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let request: IpcRequest = match serde_json::from_str(line) {
            Ok(req) => req,
            Err(e) => {
                let resp = IpcResponse::Error {
                    message: format!("Invalid JSON request: {}", e),
                };
                send_response(&mut writer, &resp).await?;
                continue;
            }
        };

        match request {
            IpcRequest::Status => match handle.get_status().await {
                Ok(status) => {
                    send_response(&mut writer, &IpcResponse::Status(status)).await?;
                }
                Err(e) => {
                    send_response(
                        &mut writer,
                        &IpcResponse::Error {
                            message: e.to_string(),
                        },
                    )
                    .await?;
                }
            },
            IpcRequest::Devices => match handle.list_devices().await {
                Ok(devices) => {
                    send_response(&mut writer, &IpcResponse::Devices { devices }).await?;
                }
                Err(e) => {
                    send_response(
                        &mut writer,
                        &IpcResponse::Error {
                            message: e.to_string(),
                        },
                    )
                    .await?;
                }
            },
            IpcRequest::Select { device_a, device_b } => {
                match handle.select_pair(device_a.clone(), device_b.clone()).await {
                    Ok(()) => {
                        send_response(
                            &mut writer,
                            &IpcResponse::Ok {
                                message: Some(format!(
                                    "Selected output pair: {} and {}",
                                    device_a, device_b
                                )),
                            },
                        )
                        .await?;
                    }
                    Err(e) => {
                        send_response(
                            &mut writer,
                            &IpcResponse::Error {
                                message: e.to_string(),
                            },
                        )
                        .await?;
                    }
                }
            }
            IpcRequest::Volume { device, volume } => {
                match handle.set_volume(device.clone(), volume).await {
                    Ok(()) => {
                        send_response(
                            &mut writer,
                            &IpcResponse::Ok {
                                message: Some(format!(
                                    "Volume for {} set to {:.2}",
                                    device, volume
                                )),
                            },
                        )
                        .await?;
                    }
                    Err(e) => {
                        send_response(
                            &mut writer,
                            &IpcResponse::Error {
                                message: e.to_string(),
                            },
                        )
                        .await?;
                    }
                }
            }
            IpcRequest::Mute { device, mute } => {
                match handle.set_mute(device.clone(), mute).await {
                    Ok(()) => {
                        send_response(
                            &mut writer,
                            &IpcResponse::Ok {
                                message: Some(format!("Mute for {} updated", device)),
                            },
                        )
                        .await?;
                    }
                    Err(e) => {
                        send_response(
                            &mut writer,
                            &IpcResponse::Error {
                                message: e.to_string(),
                            },
                        )
                        .await?;
                    }
                }
            }
            IpcRequest::Delay { device, delay_ms } => {
                match handle.set_delay(device.clone(), delay_ms).await {
                    Ok(()) => {
                        send_response(
                            &mut writer,
                            &IpcResponse::Ok {
                                message: Some(format!(
                                    "Delay for {} set to {}ms",
                                    device, delay_ms
                                )),
                            },
                        )
                        .await?;
                    }
                    Err(e) => {
                        send_response(
                            &mut writer,
                            &IpcResponse::Error {
                                message: e.to_string(),
                            },
                        )
                        .await?;
                    }
                }
            }
            IpcRequest::Mode { mode } => match handle.set_mode(mode).await {
                Ok(()) => {
                    send_response(
                        &mut writer,
                        &IpcResponse::Ok {
                            message: Some(format!("Channel mode set to {}", mode)),
                        },
                    )
                    .await?;
                }
                Err(e) => {
                    send_response(
                        &mut writer,
                        &IpcResponse::Error {
                            message: e.to_string(),
                        },
                    )
                    .await?;
                }
            },
            IpcRequest::Subscribe => {
                // Send initial status and devices
                if let Ok(st) = handle.get_status().await {
                    send_event(&mut writer, &DaemonEvent::Status(st)).await?;
                }
                if let Ok(devs) = handle.list_devices().await {
                    send_event(&mut writer, &DaemonEvent::Devices { devices: devs }).await?;
                }

                // Stream subsequent events
                while let Ok(event) = event_rx.recv().await {
                    if let Err(e) = send_event(&mut writer, &event).await {
                        warn!("Failed to stream event to subscriber: {:?}", e);
                        break;
                    }
                }
                return Ok(());
            }
        }
    }

    Ok(())
}

async fn send_response(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    resp: &IpcResponse,
) -> Result<()> {
    let mut json = serde_json::to_string(resp)?;
    json.push('\n');
    writer.write_all(json.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

async fn send_event(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    event: &DaemonEvent,
) -> Result<()> {
    let mut json = serde_json::to_string(event)?;
    json.push('\n');
    writer.write_all(json.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}
