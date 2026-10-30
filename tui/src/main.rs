use anyhow::{Context, Result};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Gauge, List, ListItem, Paragraph},
    Frame, Terminal,
};
use std::io::{self, stdout};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use twinsink_core::{
    socket_path, AudioDevice, ChannelMode, DaemonEvent, DaemonStatus, IpcRequest, IpcResponse,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveFocus {
    DeviceList,
    DeviceA,
    DeviceB,
}

struct TuiState {
    status: Option<DaemonStatus>,
    devices: Vec<AudioDevice>,
    selected_list_idx: usize,
    focus: ActiveFocus,
    status_msg: Option<(String, std::time::Instant)>,
    should_quit: bool,
}

impl TuiState {
    fn new() -> Self {
        Self {
            status: None,
            devices: Vec::new(),
            selected_list_idx: 0,
            focus: ActiveFocus::DeviceList,
            status_msg: None,
            should_quit: false,
        }
    }

    fn set_msg(&mut self, msg: impl Into<String>) {
        self.status_msg = Some((msg.into(), std::time::Instant::now()));
    }
}

async fn send_command(req: &IpcRequest) -> Result<IpcResponse> {
    let sock = socket_path();
    let stream = UnixStream::connect(&sock)
        .await
        .with_context(|| format!("Failed to connect to twinsink socket at {}", sock.display()))?;

    let (reader, mut writer) = stream.into_split();
    let mut json = serde_json::to_string(req)?;
    json.push('\n');
    writer.write_all(json.as_bytes()).await?;
    writer.flush().await?;

    let mut lines = BufReader::new(reader).lines();
    if let Some(line) = lines.next_line().await? {
        let resp: IpcResponse = serde_json::from_str(line.trim())?;
        Ok(resp)
    } else {
        anyhow::bail!("No response received from daemon")
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let sock = socket_path();
    let sub_stream = match UnixStream::connect(&sock).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "Error: twinsink daemon is not running.\nCould not connect to socket at: {}\nReason: {}\nStart the daemon with: twinsink-daemon &",
                sock.display(),
                e
            );
            std::process::exit(1);
        }
    };

    let (reader, mut writer) = sub_stream.into_split();
    // Subscribe to event stream
    let mut req_json = serde_json::to_string(&IpcRequest::Subscribe)?;
    req_json.push('\n');
    writer.write_all(req_json.as_bytes()).await?;
    writer.flush().await?;

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Handle panic hook to restore terminal
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        default_panic(info);
    }));

    let mut state = TuiState::new();
    let mut event_lines = BufReader::new(reader).lines();

    // Key event worker thread
    let (key_tx, mut key_rx) = tokio::sync::mpsc::unbounded_channel();
    std::thread::spawn(move || loop {
        if let Ok(true) = event::poll(Duration::from_millis(50)) {
            if let Ok(Event::Key(key)) = event::read() {
                if key.kind == KeyEventKind::Press && key_tx.send(key).is_err() {
                    break;
                }
            }
        }
    });

    while !state.should_quit {
        terminal.draw(|f| draw_ui(f, &state))?;

        tokio::select! {
            Some(key) = key_rx.recv() => {
                handle_key_event(key, &mut state).await;
            }
            line = event_lines.next_line() => {
                match line {
                    Ok(Some(l)) => {
                        let trimmed = l.trim();
                        if let Ok(ev) = serde_json::from_str::<DaemonEvent>(trimmed) {
                            match ev {
                                DaemonEvent::Status(st) => {
                                    state.status = Some(st);
                                }
                                DaemonEvent::Devices { devices } => {
                                    state.devices = devices;
                                    if let Some(status) = state.status.as_mut() {
                                        status.connected_a = status.device_a.as_ref().is_some_and(|name| state.devices.iter().any(|device| &device.name == name));
                                        status.connected_b = status.device_b.as_ref().is_some_and(|name| state.devices.iter().any(|device| &device.name == name));
                                    }
                                    if state.selected_list_idx >= state.devices.len() && !state.devices.is_empty() {
                                        state.selected_list_idx = state.devices.len() - 1;
                                    }
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        state.set_msg("Daemon disconnected");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        break;
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        }

        // Clear expired status messages (3 seconds)
        if let Some((_, created)) = state.status_msg {
            if created.elapsed() > Duration::from_secs(3) {
                state.status_msg = None;
            }
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}

async fn handle_key_event(key: KeyEvent, state: &mut TuiState) {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        state.should_quit = true;
        return;
    }

    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => {
            state.should_quit = true;
        }
        KeyCode::Up | KeyCode::Char('k') => {
            if state.focus == ActiveFocus::DeviceList && !state.devices.is_empty() {
                if state.selected_list_idx == 0 {
                    state.selected_list_idx = state.devices.len() - 1;
                } else {
                    state.selected_list_idx -= 1;
                }
            }
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if state.focus == ActiveFocus::DeviceList && !state.devices.is_empty() {
                if state.selected_list_idx + 1 >= state.devices.len() {
                    state.selected_list_idx = 0;
                } else {
                    state.selected_list_idx += 1;
                }
            }
        }
        KeyCode::Tab => {
            state.focus = match state.focus {
                ActiveFocus::DeviceList => ActiveFocus::DeviceA,
                ActiveFocus::DeviceA => ActiveFocus::DeviceB,
                ActiveFocus::DeviceB => ActiveFocus::DeviceList,
            };
        }
        KeyCode::Char('1') => {
            // Assign highlighted device to Device A
            if let Some(target) = state.devices.get(state.selected_list_idx) {
                let current_b = state.status.as_ref().and_then(|s| s.device_b.clone());
                if current_b.as_deref() != Some(&target.name) {
                    let req = IpcRequest::Select {
                        device_a: target.name.clone(),
                        device_b: current_b.unwrap_or_default(),
                    };
                    match send_command(&req).await {
                        Ok(IpcResponse::Ok { message }) => {
                            state.set_msg(message.unwrap_or_else(|| "Updated output pair".into()))
                        }
                        Ok(IpcResponse::Error { message }) => state.set_msg(message),
                        Err(e) => state.set_msg(e.to_string()),
                        _ => {}
                    }
                } else {
                    state.set_msg("That device is already assigned to B");
                }
            }
        }
        KeyCode::Char('2') => {
            // Assign highlighted device to Device B
            if let Some(target) = state.devices.get(state.selected_list_idx) {
                let current_a = state.status.as_ref().and_then(|s| s.device_a.clone());
                if current_a.as_deref() != Some(&target.name) {
                    let req = IpcRequest::Select {
                        device_a: current_a.unwrap_or_default(),
                        device_b: target.name.clone(),
                    };
                    match send_command(&req).await {
                        Ok(IpcResponse::Ok { message }) => {
                            state.set_msg(message.unwrap_or_else(|| "Updated output pair".into()))
                        }
                        Ok(IpcResponse::Error { message }) => state.set_msg(message),
                        Err(e) => state.set_msg(e.to_string()),
                        _ => {}
                    }
                } else {
                    state.set_msg("That device is already assigned to A");
                }
            }
        }
        KeyCode::Char('u') => {
            let (device_a, device_b) = state
                .status
                .as_ref()
                .map(|s| (s.device_a.clone(), s.device_b.clone()))
                .unwrap_or_default();
            let (device_a, device_b) = match state.focus {
                ActiveFocus::DeviceA => (None, device_b),
                ActiveFocus::DeviceB => (device_a, None),
                ActiveFocus::DeviceList => {
                    state.set_msg("Tab to output A or B, then press u to unassign it");
                    return;
                }
            };
            let req = IpcRequest::Select {
                device_a: device_a.unwrap_or_default(),
                device_b: device_b.unwrap_or_default(),
            };
            match send_command(&req).await {
                Ok(IpcResponse::Ok { message }) => {
                    state.set_msg(message.unwrap_or_else(|| "Output unassigned".into()))
                }
                Ok(IpcResponse::Error { message }) => state.set_msg(message),
                Err(e) => state.set_msg(e.to_string()),
                _ => {}
            }
        }
        KeyCode::Char('c') | KeyCode::Char('m') if key.modifiers.is_empty() => {
            // Cycle channel mode
            if let Some(ref st) = state.status {
                let next_mode = match st.mode {
                    ChannelMode::Duplicate => ChannelMode::Split,
                    ChannelMode::Split => ChannelMode::Mono,
                    ChannelMode::Mono => ChannelMode::Duplicate,
                };
                let req = IpcRequest::Mode { mode: next_mode };
                if let Ok(IpcResponse::Ok { message }) = send_command(&req).await {
                    state.set_msg(message.unwrap_or_else(|| format!("Mode: {}", next_mode)));
                }
            }
        }
        KeyCode::Char('+') | KeyCode::Char('=') => {
            // Volume up on focused device
            adjust_volume(state, 0.05).await;
        }
        KeyCode::Char('-') | KeyCode::Char('_') => {
            // Volume down on focused device
            adjust_volume(state, -0.05).await;
        }
        KeyCode::Char('x') => {
            // Toggle mute on focused device
            toggle_mute(state).await;
        }
        KeyCode::Left | KeyCode::Char('[') => {
            // Delay decrease by 10ms
            adjust_delay(state, -10).await;
        }
        KeyCode::Right | KeyCode::Char(']') => {
            // Delay increase by 10ms
            adjust_delay(state, 10).await;
        }
        _ => {}
    }
}

async fn adjust_volume(state: &mut TuiState, delta: f32) {
    let (target_dev, cur_vol) = match state.focus {
        ActiveFocus::DeviceB => {
            let dev = state.status.as_ref().and_then(|s| s.device_b.clone());
            let vol = state.status.as_ref().map(|s| s.volume_b).unwrap_or(1.0);
            (dev, vol)
        }
        _ => {
            let dev = state.status.as_ref().and_then(|s| s.device_a.clone());
            let vol = state.status.as_ref().map(|s| s.volume_a).unwrap_or(1.0);
            (dev, vol)
        }
    };

    if let Some(dev) = target_dev {
        let new_vol = (cur_vol + delta).clamp(0.0, 1.0);
        let req = IpcRequest::Volume {
            device: dev.clone(),
            volume: new_vol,
        };
        let _ = send_command(&req).await;
    }
}

async fn toggle_mute(state: &mut TuiState) {
    let (target_dev, cur_mute) = match state.focus {
        ActiveFocus::DeviceB => {
            let dev = state.status.as_ref().and_then(|s| s.device_b.clone());
            let mute = state.status.as_ref().map(|s| s.mute_b).unwrap_or(false);
            (dev, mute)
        }
        _ => {
            let dev = state.status.as_ref().and_then(|s| s.device_a.clone());
            let mute = state.status.as_ref().map(|s| s.mute_a).unwrap_or(false);
            (dev, mute)
        }
    };

    if let Some(dev) = target_dev {
        let req = IpcRequest::Mute {
            device: dev,
            mute: Some(!cur_mute),
        };
        let _ = send_command(&req).await;
    }
}

async fn adjust_delay(state: &mut TuiState, delta_ms: i32) {
    let (target_dev, cur_delay) = match state.focus {
        ActiveFocus::DeviceB => {
            let dev = state.status.as_ref().and_then(|s| s.device_b.clone());
            let delay = state.status.as_ref().map(|s| s.delay_ms_b).unwrap_or(0);
            (dev, delay)
        }
        _ => {
            let dev = state.status.as_ref().and_then(|s| s.device_a.clone());
            let delay = state.status.as_ref().map(|s| s.delay_ms_a).unwrap_or(0);
            (dev, delay)
        }
    };

    if let Some(dev) = target_dev {
        let new_delay = if delta_ms < 0 {
            cur_delay.saturating_sub((-delta_ms) as u32)
        } else {
            (cur_delay + delta_ms as u32).min(2000)
        };
        let req = IpcRequest::Delay {
            device: dev,
            delay_ms: new_delay,
        };
        let _ = send_command(&req).await;
    }
}

fn draw_ui(f: &mut Frame, state: &TuiState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(10),   // Body
            Constraint::Length(3), // Footer
        ])
        .split(f.area());

    // 1. Header
    draw_header(f, chunks[0], state);

    // 2. Body: Left = Devices List, Right = Controls
    let body_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(45), // Sinks list
            Constraint::Percentage(55), // Outputs & controls
        ])
        .split(chunks[1]);

    draw_device_list(f, body_chunks[0], state);
    draw_controls(f, body_chunks[1], state);

    // 3. Footer
    draw_footer(f, chunks[2], state);
}

fn draw_header(f: &mut Frame, area: Rect, state: &TuiState) {
    let mode_str = state
        .status
        .as_ref()
        .map(|s| s.mode.to_string())
        .unwrap_or_else(|| "unknown".into());

    let header_text = vec![Line::from(vec![
        Span::styled(
            " TwinSink ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("─ Multi-Output Audio Router  │  Virtual Sink: "),
        Span::styled(
            "twinsink",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  │  Mode: "),
        Span::styled(
            mode_str.to_uppercase(),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    ])];

    let header_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray));

    let paragraph = Paragraph::new(header_text)
        .block(header_block)
        .alignment(Alignment::Left);
    f.render_widget(paragraph, area);
}

fn draw_device_list(f: &mut Frame, area: Rect, state: &TuiState) {
    let active_a = state.status.as_ref().and_then(|s| s.device_a.as_deref());
    let active_b = state.status.as_ref().and_then(|s| s.device_b.as_deref());

    let border_style = if state.focus == ActiveFocus::DeviceList {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let items: Vec<ListItem> = state
        .devices
        .iter()
        .enumerate()
        .map(|(idx, dev)| {
            let is_a = active_a == Some(&dev.name);
            let is_b = active_b == Some(&dev.name);
            let is_selected = idx == state.selected_list_idx;

            let badge = if is_a && is_b {
                Span::styled(
                    "[A+B] ",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )
            } else if is_a {
                Span::styled(
                    " [A]  ",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )
            } else if is_b {
                Span::styled(
                    " [B]  ",
                    Style::default()
                        .fg(Color::Magenta)
                        .add_modifier(Modifier::BOLD),
                )
            } else {
                Span::styled(" [ ]  ", Style::default().fg(Color::DarkGray))
            };

            let prefix = if is_selected {
                Span::styled("▶ ", Style::default().fg(Color::Yellow))
            } else {
                Span::raw("  ")
            };

            let name_style = if is_selected {
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD)
            } else if is_a || is_b {
                Style::default().fg(Color::White)
            } else {
                Style::default().fg(Color::Gray)
            };

            ListItem::new(Line::from(vec![
                prefix,
                badge,
                Span::styled(dev.description.clone(), name_style),
            ]))
        })
        .collect();

    let title = format!(" Discovered Audio Sinks ({}) ", state.devices.len());
    let list_widget = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(border_style)
            .title(title),
    );

    f.render_widget(list_widget, area);
}

fn draw_controls(f: &mut Frame, area: Rect, state: &TuiState) {
    let ctrl_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(45), // Device A
            Constraint::Percentage(45), // Device B
            Constraint::Percentage(10), // Channel mode row
        ])
        .split(area);

    // Device A box
    draw_device_card(
        f,
        ctrl_chunks[0],
        DeviceCardProps {
            title: "Output Device A (Primary)",
            name: state.status.as_ref().and_then(|s| s.device_a.as_deref()),
            connected: state
                .status
                .as_ref()
                .map(|s| s.connected_a)
                .unwrap_or(false),
            volume: state.status.as_ref().map(|s| s.volume_a).unwrap_or(1.0),
            mute: state.status.as_ref().map(|s| s.mute_a).unwrap_or(false),
            delay_ms: state.status.as_ref().map(|s| s.delay_ms_a).unwrap_or(0),
            is_focused: state.focus == ActiveFocus::DeviceA,
            theme_color: Color::Cyan,
        },
    );

    // Device B box
    draw_device_card(
        f,
        ctrl_chunks[1],
        DeviceCardProps {
            title: "Output Device B (Secondary)",
            name: state.status.as_ref().and_then(|s| s.device_b.as_deref()),
            connected: state
                .status
                .as_ref()
                .map(|s| s.connected_b)
                .unwrap_or(false),
            volume: state.status.as_ref().map(|s| s.volume_b).unwrap_or(1.0),
            mute: state.status.as_ref().map(|s| s.mute_b).unwrap_or(false),
            delay_ms: state.status.as_ref().map(|s| s.delay_ms_b).unwrap_or(0),
            is_focused: state.focus == ActiveFocus::DeviceB,
            theme_color: Color::Magenta,
        },
    );

    // Mode Selector box
    draw_mode_row(f, ctrl_chunks[2], state);
}

struct DeviceCardProps<'a> {
    title: &'a str,
    name: Option<&'a str>,
    connected: bool,
    volume: f32,
    mute: bool,
    delay_ms: u32,
    is_focused: bool,
    theme_color: Color,
}

fn draw_device_card(f: &mut Frame, area: Rect, props: DeviceCardProps) {
    let border_style = if props.is_focused {
        Style::default()
            .fg(props.theme_color)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let title_prefix = if props.is_focused { "▶ " } else { "  " };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(border_style)
        .title(format!("{}{}", title_prefix, props.title));

    let inner = block.inner(area);
    f.render_widget(block, area);

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Target name & status
            Constraint::Length(2), // Volume
            Constraint::Length(2), // Delay
        ])
        .split(inner);

    // Row 0: Target Name & Connection dot
    let status_dot = if props.connected {
        Span::styled(
            "● Connected",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        Span::styled("○ Disconnected", Style::default().fg(Color::Red))
    };

    let dev_label = props.name.unwrap_or("None selected");
    let name_line = Line::from(vec![
        Span::raw("Device: "),
        Span::styled(
            dev_label,
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  ["),
        status_dot,
        Span::raw("]"),
    ]);
    f.render_widget(Paragraph::new(name_line), rows[0]);

    // Row 1: Volume gauge & mute
    let mute_tag = if props.mute {
        Span::styled(
            " [MUTED] ",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        )
    } else {
        Span::styled(" [UNMUTED] ", Style::default().fg(Color::Green))
    };

    let vol_percent = (props.volume * 100.0).round() as u16;
    let vol_gauge = Gauge::default()
        .block(
            Block::default()
                .title(Line::from(vec![Span::raw("Volume "), mute_tag]))
                .borders(Borders::NONE),
        )
        .gauge_style(
            Style::default()
                .fg(if props.mute {
                    Color::DarkGray
                } else {
                    props.theme_color
                })
                .bg(Color::Rgb(30, 30, 30)),
        )
        .percent(vol_percent)
        .label(format!("{}%", vol_percent));
    f.render_widget(vol_gauge, rows[1]);

    // Row 2: Delay slider
    let delay_ratio = (props.delay_ms as f64 / 1000.0).clamp(0.0, 1.0);
    let delay_gauge = Gauge::default()
        .block(
            Block::default()
                .title(format!("Delay: {} ms", props.delay_ms))
                .borders(Borders::NONE),
        )
        .gauge_style(
            Style::default()
                .fg(Color::Yellow)
                .bg(Color::Rgb(30, 30, 30)),
        )
        .ratio(delay_ratio)
        .label(format!("{} ms", props.delay_ms));
    f.render_widget(delay_gauge, rows[2]);
}

fn draw_mode_row(f: &mut Frame, area: Rect, state: &TuiState) {
    let current_mode = state
        .status
        .as_ref()
        .map(|s| s.mode)
        .unwrap_or(ChannelMode::Duplicate);

    let modes = [
        (ChannelMode::Duplicate, "1: Duplicate (Stereo)"),
        (ChannelMode::Split, "2: Split (L->A, R->B)"),
        (ChannelMode::Mono, "3: Mono Downmix"),
    ];

    let mut spans = vec![Span::raw(" Channel Mode: ")];
    for (m, label) in modes {
        if m == current_mode {
            spans.push(Span::styled(
                format!(" [{}] ", label),
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ));
        } else {
            spans.push(Span::styled(
                format!("  {}  ", label),
                Style::default().fg(Color::Gray),
            ));
        }
        spans.push(Span::raw(" "));
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray));

    let paragraph = Paragraph::new(Line::from(spans)).block(block);
    f.render_widget(paragraph, area);
}

fn draw_footer(f: &mut Frame, area: Rect, state: &TuiState) {
    let help_line = Line::from(vec![
        Span::styled("[q]", Style::default().fg(Color::Yellow)),
        Span::raw(" Quit  "),
        Span::styled("[j/k]", Style::default().fg(Color::Yellow)),
        Span::raw(" Nav  "),
        Span::styled("[1]", Style::default().fg(Color::Cyan)),
        Span::raw(" Assign A  "),
        Span::styled("[2]", Style::default().fg(Color::Magenta)),
        Span::raw(" Assign B  "),
        Span::styled("[Tab]", Style::default().fg(Color::Yellow)),
        Span::raw(" Focus A/B  "),
        Span::styled("[u]", Style::default().fg(Color::Yellow)),
        Span::raw(" Unassign focused  "),
        Span::styled("[+/-]", Style::default().fg(Color::Yellow)),
        Span::raw(" Vol  "),
        Span::styled("[x]", Style::default().fg(Color::Yellow)),
        Span::raw(" Mute  "),
        Span::styled("[←/→]", Style::default().fg(Color::Yellow)),
        Span::raw(" Delay  "),
        Span::styled("[c]", Style::default().fg(Color::Yellow)),
        Span::raw(" Mode"),
    ]);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray));

    let paragraph = if let Some((ref msg, _)) = state.status_msg {
        Paragraph::new(Line::from(vec![
            Span::styled("Notice: ", Style::default().fg(Color::Yellow)),
            Span::styled(
                msg.clone(),
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
        ]))
        .block(block)
    } else {
        Paragraph::new(help_line).block(block)
    };

    f.render_widget(paragraph, area);
}
