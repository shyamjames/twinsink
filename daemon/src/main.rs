mod pipewire_thread;

use anyhow::Result;
use pipewire_thread::{PipewireHandle, PwEvent};
use tokio::sync::mpsc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting twinsink daemon (Phase 2: Device discovery)");

    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let (handle, _cmd_tx) = PipewireHandle::spawn(event_tx)?;

    // Give PipeWire loop a moment to collect registry globals
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    let devices = handle.list_devices().await?;
    info!("Discovered {} audio sinks:", devices.len());
    for dev in &devices {
        info!(
            "  - [{}] {} ({:?}): {}",
            dev.id, dev.name, dev.state, dev.description
        );
    }

    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);

    loop {
        tokio::select! {
            _ = &mut shutdown_signal => {
                info!("Shutdown signal received");
                break;
            }
            Some(event) = event_rx.recv() => {
                match event {
                    PwEvent::Added(dev) => {
                        info!("Event: Added -> {} ({})", dev.name, dev.description);
                    }
                    PwEvent::Removed { id, name } => {
                        info!("Event: Removed -> [{}] {}", id, name);
                    }
                    PwEvent::Changed(dev) => {
                        info!("Event: Changed -> {} state={:?}", dev.name, dev.state);
                    }
                }
            }
        }
    }

    handle.shutdown()?;
    info!("twinsink daemon stopped");
    Ok(())
}
