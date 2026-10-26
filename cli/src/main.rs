use anyhow::Result;
use clap::{Parser, Subcommand};
use std::process::ExitCode;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use twinsink_core::{socket_path, AudioDevice, DaemonEvent, DaemonStatus, IpcRequest, IpcResponse};

#[derive(Parser, Debug)]
#[command(
    name = "twinsink",
    about = "Control CLI for twinsink multi-output audio router"
)]
struct Cli {
    #[arg(
        long,
        global = true,
        help = "Output raw JSON instead of human-readable text"
    )]
    json: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[command(about = "Show current router status and active outputs")]
    Status,

    #[command(about = "List all detected PipeWire audio sinks")]
    Devices,

    #[command(about = "Select two output sinks to route audio to")]
    Select {
        #[arg(help = "Primary sink name (node.name)")]
        device_a: String,
        #[arg(help = "Secondary sink name (node.name)")]
        device_b: String,
    },

    #[command(about = "Set volume for a selected device (0.0 to 1.0)")]
    Volume {
        #[arg(help = "Target sink name (node.name)")]
        device: String,
        #[arg(help = "Volume level (0.0 to 1.0)")]
        volume: f32,
    },

    #[command(about = "Toggle or set mute for a selected device")]
    Mute {
        #[arg(help = "Target sink name (node.name)")]
        device: String,
        #[arg(long, help = "Explicit mute state: true to mute, false to unmute")]
        state: Option<bool>,
    },

    #[command(about = "Subscribe to live event stream from the daemon")]
    Subscribe,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    let sock_path = socket_path();
    let stream = match UnixStream::connect(&sock_path).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "Error: twinsink daemon is not running.\nCould not connect to socket at: {}\nReason: {}\nStart the daemon with: twinsink-daemon &",
                sock_path.display(),
                e
            );
            return ExitCode::FAILURE;
        }
    };

    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    match cli.command {
        Commands::Status => {
            let req = IpcRequest::Status;
            if let Err(e) = send_request(&mut writer, &req).await {
                eprintln!("Error sending request: {}", e);
                return ExitCode::FAILURE;
            }

            match read_response(&mut lines).await {
                Ok(IpcResponse::Status(status)) => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&status).unwrap_or_default()
                        );
                    } else {
                        print_status(&status);
                    }
                    ExitCode::SUCCESS
                }
                Ok(IpcResponse::Error { message }) => {
                    eprintln!("Daemon error: {}", message);
                    ExitCode::FAILURE
                }
                Ok(other) => {
                    eprintln!("Unexpected response: {:?}", other);
                    ExitCode::FAILURE
                }
                Err(e) => {
                    eprintln!("Error reading response: {}", e);
                    ExitCode::FAILURE
                }
            }
        }
        Commands::Devices => {
            let req = IpcRequest::Devices;
            if let Err(e) = send_request(&mut writer, &req).await {
                eprintln!("Error sending request: {}", e);
                return ExitCode::FAILURE;
            }

            match read_response(&mut lines).await {
                Ok(IpcResponse::Devices { devices }) => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&devices).unwrap_or_default()
                        );
                    } else {
                        print_devices(&devices);
                    }
                    ExitCode::SUCCESS
                }
                Ok(IpcResponse::Error { message }) => {
                    eprintln!("Daemon error: {}", message);
                    ExitCode::FAILURE
                }
                Ok(other) => {
                    eprintln!("Unexpected response: {:?}", other);
                    ExitCode::FAILURE
                }
                Err(e) => {
                    eprintln!("Error reading response: {}", e);
                    ExitCode::FAILURE
                }
            }
        }
        Commands::Select { device_a, device_b } => {
            let req = IpcRequest::Select {
                device_a: device_a.clone(),
                device_b: device_b.clone(),
            };
            if let Err(e) = send_request(&mut writer, &req).await {
                eprintln!("Error sending request: {}", e);
                return ExitCode::FAILURE;
            }

            match read_response(&mut lines).await {
                Ok(IpcResponse::Ok { message }) => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "status": "ok",
                                "device_a": device_a,
                                "device_b": device_b,
                                "message": message
                            })
                        );
                    } else {
                        println!(
                            "{}",
                            message.unwrap_or_else(|| {
                                format!("Selected pair: {} and {}", device_a, device_b)
                            })
                        );
                    }
                    ExitCode::SUCCESS
                }
                Ok(IpcResponse::Error { message }) => {
                    eprintln!("Error: {}", message);
                    ExitCode::FAILURE
                }
                Ok(other) => {
                    eprintln!("Unexpected response: {:?}", other);
                    ExitCode::FAILURE
                }
                Err(e) => {
                    eprintln!("Error reading response: {}", e);
                    ExitCode::FAILURE
                }
            }
        }
        Commands::Volume { device, volume } => {
            let req = IpcRequest::Volume {
                device: device.clone(),
                volume,
            };
            if let Err(e) = send_request(&mut writer, &req).await {
                eprintln!("Error sending request: {}", e);
                return ExitCode::FAILURE;
            }

            match read_response(&mut lines).await {
                Ok(IpcResponse::Ok { message }) => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "status": "ok",
                                "device": device,
                                "volume": volume,
                                "message": message
                            })
                        );
                    } else {
                        println!(
                            "{}",
                            message.unwrap_or_else(|| format!(
                                "Volume for {} set to {:.2}",
                                device, volume
                            ))
                        );
                    }
                    ExitCode::SUCCESS
                }
                Ok(IpcResponse::Error { message }) => {
                    eprintln!("Error: {}", message);
                    ExitCode::FAILURE
                }
                Ok(other) => {
                    eprintln!("Unexpected response: {:?}", other);
                    ExitCode::FAILURE
                }
                Err(e) => {
                    eprintln!("Error reading response: {}", e);
                    ExitCode::FAILURE
                }
            }
        }
        Commands::Mute { device, state } => {
            let req = IpcRequest::Mute {
                device: device.clone(),
                mute: state,
            };
            if let Err(e) = send_request(&mut writer, &req).await {
                eprintln!("Error sending request: {}", e);
                return ExitCode::FAILURE;
            }

            match read_response(&mut lines).await {
                Ok(IpcResponse::Ok { message }) => {
                    if cli.json {
                        println!(
                            "{}",
                            serde_json::json!({
                                "status": "ok",
                                "device": device,
                                "mute": state,
                                "message": message
                            })
                        );
                    } else {
                        println!(
                            "{}",
                            message.unwrap_or_else(|| format!("Mute updated for {}", device))
                        );
                    }
                    ExitCode::SUCCESS
                }
                Ok(IpcResponse::Error { message }) => {
                    eprintln!("Error: {}", message);
                    ExitCode::FAILURE
                }
                Ok(other) => {
                    eprintln!("Unexpected response: {:?}", other);
                    ExitCode::FAILURE
                }
                Err(e) => {
                    eprintln!("Error reading response: {}", e);
                    ExitCode::FAILURE
                }
            }
        }
        Commands::Subscribe => {
            let req = IpcRequest::Subscribe;
            if let Err(e) = send_request(&mut writer, &req).await {
                eprintln!("Error sending request: {}", e);
                return ExitCode::FAILURE;
            }

            while let Ok(Some(line)) = lines.next_line().await {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                if cli.json {
                    println!("{}", trimmed);
                } else if let Ok(event) = serde_json::from_str::<DaemonEvent>(trimmed) {
                    match event {
                        DaemonEvent::Status(st) => {
                            println!(
                                "[EVENT] Status: pair=({:?}, {:?}) connected=({}, {}) mode={}",
                                st.device_a, st.device_b, st.connected_a, st.connected_b, st.mode
                            );
                        }
                        DaemonEvent::Devices { devices } => {
                            println!("[EVENT] Devices updated: count={}", devices.len());
                        }
                    }
                }
            }
            ExitCode::SUCCESS
        }
    }
}

async fn send_request(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    req: &IpcRequest,
) -> Result<()> {
    let mut json = serde_json::to_string(req)?;
    json.push('\n');
    writer.write_all(json.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_response(
    lines: &mut tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
) -> Result<IpcResponse> {
    if let Some(line) = lines.next_line().await? {
        let resp: IpcResponse = serde_json::from_str(line.trim())?;
        Ok(resp)
    } else {
        anyhow::bail!("Connection closed by daemon")
    }
}

fn print_status(status: &DaemonStatus) {
    println!("TwinSink Router Status");
    println!("----------------------");
    println!("Virtual Sink: {}", status.virtual_sink);
    println!("Channel Mode: {}", status.mode);
    println!();

    println!("Device A (Primary):");
    if let Some(ref a) = status.device_a {
        println!("  Name:      {}", a);
        println!(
            "  Connected: {}",
            if status.connected_a { "yes" } else { "no" }
        );
        println!("  Volume:    {:.0}%", status.volume_a * 100.0);
        println!("  Muted:     {}", if status.mute_a { "yes" } else { "no" });
        println!("  Delay:     {} ms", status.delay_ms_a);
    } else {
        println!("  None selected");
    }

    println!();
    println!("Device B (Secondary):");
    if let Some(ref b) = status.device_b {
        println!("  Name:      {}", b);
        println!(
            "  Connected: {}",
            if status.connected_b { "yes" } else { "no" }
        );
        println!("  Volume:    {:.0}%", status.volume_b * 100.0);
        println!("  Muted:     {}", if status.mute_b { "yes" } else { "no" });
        println!("  Delay:     {} ms", status.delay_ms_b);
    } else {
        println!("  None selected");
    }
}

fn print_devices(devices: &[AudioDevice]) {
    println!("Detected Audio Sinks ({})", devices.len());
    println!("------------------------");
    for dev in devices {
        let state_str = match dev.state {
            twinsink_core::DeviceState::Active => "Active",
            twinsink_core::DeviceState::Idle => "Idle",
            twinsink_core::DeviceState::Suspended => "Suspended",
            twinsink_core::DeviceState::Unknown => "Unknown",
        };
        println!("[{}] {} ({})", dev.id, dev.name, state_str);
        println!("    Description: {}", dev.description);
    }
}
