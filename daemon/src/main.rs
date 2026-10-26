mod pipewire_thread;

use anyhow::Result;
use clap::Parser;
use pipewire_thread::{PipewireHandle, PwEvent};
use tokio::sync::mpsc;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(
    name = "twinsink-daemon",
    about = "Headless multi-output audio router daemon for Linux"
)]
struct Args {
    #[arg(long, help = "Primary output device (node.name)")]
    device_a: Option<String>,

    #[arg(long, help = "Secondary output device (node.name)")]
    device_b: Option<String>,

    #[arg(long, help = "Run only startup crash check & restore, then exit")]
    restore_only: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();
    info!("Starting twinsink daemon (Phase 3: Virtual sink + linking)");

    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let (handle, _cmd_tx) = PipewireHandle::spawn(event_tx)?;

    // Allow PipeWire to collect registry globals and perform startup recovery check
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    if args.restore_only {
        info!("Restore-only requested, shutting down");
        handle.restore_default().await?;
        handle.shutdown()?;
        return Ok(());
    }

    let devices = handle.list_devices().await?;
    info!("Discovered {} audio sinks:", devices.len());
    for dev in &devices {
        info!(
            "  - [{}] {} ({:?}): {}",
            dev.id, dev.name, dev.state, dev.description
        );
    }

    // Determine device pair: CLI flags or first two available sinks
    let (dev_a, dev_b) = match (args.device_a, args.device_b) {
        (Some(a), Some(b)) => (Some(a), Some(b)),
        (Some(a), None) => {
            let b = devices.iter().find(|d| d.name != a).map(|d| d.name.clone());
            (Some(a), b)
        }
        (None, Some(b)) => {
            let a = devices.iter().find(|d| d.name != b).map(|d| d.name.clone());
            (a, Some(b))
        }
        (None, None) => {
            if devices.len() >= 2 {
                (Some(devices[0].name.clone()), Some(devices[1].name.clone()))
            } else if devices.len() == 1 {
                (Some(devices[0].name.clone()), None)
            } else {
                (None, None)
            }
        }
    };

    if let (Some(a), Some(b)) = (&dev_a, &dev_b) {
        info!(
            device_a = %a,
            device_b = %b,
            "Selecting output pair and linking twinsink"
        );
        if let Err(e) = handle.select_pair(a.clone(), b.clone()).await {
            warn!("Failed to select initial pair: {:?}", e);
        }
    } else {
        warn!("Less than two audio devices available; waiting for selection/hotplug");
    }

    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);

    loop {
        tokio::select! {
            _ = &mut shutdown_signal => {
                info!("Shutdown signal received, restoring default sink...");
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
                    PwEvent::Status(status) => {
                        info!(
                            "Status update: pair=({:?}, {:?}) connected=({}, {})",
                            status.device_a, status.device_b, status.connected_a, status.connected_b
                        );
                    }
                }
            }
        }
    }

    handle.shutdown()?;
    info!("twinsink daemon stopped cleanly");
    Ok(())
}
