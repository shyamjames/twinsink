use gtk4::prelude::*;
use gtk4::{
    Application, ApplicationWindow, Box as GtkBox, Button, ComboBoxText, Label, Orientation,
};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use twinsink_core::{socket_path, AudioDevice, DaemonStatus, IpcRequest, IpcResponse};

fn request(req: IpcRequest) -> Result<IpcResponse, String> {
    let mut stream = UnixStream::connect(socket_path())
        .map_err(|e| format!("Cannot connect to TwinSink daemon: {e}"))?;
    let mut line = serde_json::to_string(&req).map_err(|e| e.to_string())?;
    line.push('\n');
    stream
        .write_all(line.as_bytes())
        .map_err(|e| e.to_string())?;
    let mut response = String::new();
    BufReader::new(stream)
        .read_line(&mut response)
        .map_err(|e| e.to_string())?;
    serde_json::from_str(&response).map_err(|e| e.to_string())
}

fn get_devices() -> Result<Vec<AudioDevice>, String> {
    match request(IpcRequest::Devices)? {
        IpcResponse::Devices { devices } => Ok(devices),
        IpcResponse::Error { message } => Err(message),
        other => Err(format!("Unexpected daemon response: {other:?}")),
    }
}
fn get_status() -> Result<DaemonStatus, String> {
    match request(IpcRequest::Status)? {
        IpcResponse::Status(status) => Ok(status),
        IpcResponse::Error { message } => Err(message),
        other => Err(format!("Unexpected daemon response: {other:?}")),
    }
}

fn main() {
    let app = Application::builder()
        .application_id("org.twinsink.Router")
        .build();
    app.connect_activate(|app| {
        let window = ApplicationWindow::builder()
            .application(app)
            .title("TwinSink Audio Router")
            .default_width(560)
            .default_height(420)
            .build();
        let root = GtkBox::new(Orientation::Vertical, 12);
        root.set_margin_top(20);
        root.set_margin_bottom(20);
        root.set_margin_start(20);
        root.set_margin_end(20);
        root.append(&Label::new(Some(
            "Choose an output for each slot. Unassign either slot at any time.",
        )));
        let status_label = Label::new(Some("Loading TwinSink…"));
        root.append(&status_label);
        let slots = GtkBox::new(Orientation::Vertical, 10);
        let (a_row, a_combo, a_clear, a_mute) = slot_row("Output A");
        let (b_row, b_combo, b_clear, b_mute) = slot_row("Output B");
        slots.append(&a_row);
        slots.append(&b_row);
        root.append(&slots);
        let message = Label::new(None);
        message.set_wrap(true);
        root.append(&message);
        let apply = Button::with_label("Apply outputs");
        root.append(&apply);
        let refresh = Button::with_label("Refresh devices");
        root.append(&refresh);
        window.set_child(Some(&root));

        let refresh_ui = {
            let a_combo = a_combo.clone();
            let b_combo = b_combo.clone();
            let status_label = status_label.clone();
            let message = message.clone();
            let a_mute = a_mute.clone();
            let b_mute = b_mute.clone();
            move || match (get_devices(), get_status()) {
                (Ok(devices), Ok(status)) => {
                    fill_combo(&a_combo, &devices, status.device_a.as_deref());
                    fill_combo(&b_combo, &devices, status.device_b.as_deref());
                    a_mute.set_label(if status.mute_a { "Unmute" } else { "Mute" });
                    a_mute.set_sensitive(status.device_a.is_some());
                    b_mute.set_label(if status.mute_b { "Unmute" } else { "Mute" });
                    b_mute.set_sensitive(status.device_b.is_some());
                    status_label.set_text(&format!(
                        "A: {}{}   •   B: {}{}",
                        connection(&status.device_a, status.connected_a),
                        muted_label(status.mute_a),
                        connection(&status.device_b, status.connected_b),
                        muted_label(status.mute_b),
                    ));
                    message.set_text("");
                }
                (Err(e), _) | (_, Err(e)) => {
                    status_label.set_text("Daemon unavailable");
                    message.set_text(&format!("{e}\nStart twinsink-daemon and refresh."));
                }
            }
        };
        refresh_ui();
        let submit = {
            let a_combo = a_combo.clone();
            let b_combo = b_combo.clone();
            let message = message.clone();
            let refresh_ui = refresh_ui.clone();
            move || {
                let a = a_combo
                    .active_id()
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                let b = b_combo
                    .active_id()
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                if !a.is_empty() && a == b {
                    message.set_text("Choose different devices for outputs A and B.");
                    return;
                }
                match request(IpcRequest::Select {
                    device_a: a,
                    device_b: b,
                }) {
                    Ok(IpcResponse::Ok { message: response }) => {
                        message.set_text(response.as_deref().unwrap_or("Outputs updated"));
                        refresh_ui();
                    }
                    Ok(IpcResponse::Error { message: error }) => message.set_text(&error),
                    Err(error) => message.set_text(&error),
                    _ => message.set_text("Unexpected daemon response"),
                }
            }
        };
        {
            let submit = submit.clone();
            apply.connect_clicked(move |_| submit());
        }
        {
            let a_combo = a_combo.clone();
            let submit = submit.clone();
            a_clear.connect_clicked(move |_| {
                a_combo.set_active_id(Some(""));
                submit();
            });
        }
        {
            let b_combo = b_combo.clone();
            let submit = submit.clone();
            b_clear.connect_clicked(move |_| {
                b_combo.set_active_id(Some(""));
                submit();
            });
        }
        {
            let message = message.clone();
            let refresh_ui = refresh_ui.clone();
            a_mute.connect_clicked(move |_| match get_status() {
                Ok(status) => match status.device_a {
                    Some(device) => match request(IpcRequest::Mute {
                        device,
                        mute: Some(!status.mute_a),
                    }) {
                        Ok(IpcResponse::Ok { .. }) => refresh_ui(),
                        Ok(IpcResponse::Error { message: error }) => message.set_text(&error),
                        Err(error) => message.set_text(&error),
                        _ => message.set_text("Unexpected daemon response"),
                    },
                    None => message.set_text("Output A is unassigned"),
                },
                Err(error) => message.set_text(&error),
            });
        }
        {
            let message = message.clone();
            let refresh_ui = refresh_ui.clone();
            b_mute.connect_clicked(move |_| match get_status() {
                Ok(status) => match status.device_b {
                    Some(device) => match request(IpcRequest::Mute {
                        device,
                        mute: Some(!status.mute_b),
                    }) {
                        Ok(IpcResponse::Ok { .. }) => refresh_ui(),
                        Ok(IpcResponse::Error { message: error }) => message.set_text(&error),
                        Err(error) => message.set_text(&error),
                        _ => message.set_text("Unexpected daemon response"),
                    },
                    None => message.set_text("Output B is unassigned"),
                },
                Err(error) => message.set_text(&error),
            });
        }
        {
            let refresh_ui = refresh_ui.clone();
            refresh.connect_clicked(move |_| refresh_ui());
        }
        window.present();
    });
    app.run();
}

fn slot_row(title: &str) -> (GtkBox, ComboBoxText, Button, Button) {
    let row = GtkBox::new(Orientation::Horizontal, 8);
    let label = Label::new(Some(title));
    label.set_width_chars(10);
    let combo = ComboBoxText::new();
    combo.set_hexpand(true);
    let clear = Button::with_label("Unassign");
    let mute = Button::with_label("Mute");
    mute.set_sensitive(false);
    row.append(&label);
    row.append(&combo);
    row.append(&mute);
    row.append(&clear);
    (row, combo, clear, mute)
}
fn fill_combo(combo: &ComboBoxText, devices: &[AudioDevice], selected: Option<&str>) {
    combo.remove_all();
    combo.append(Some(""), "Unassigned");
    for device in devices {
        combo.append(Some(&device.name), &device.description);
    }
    combo.set_active_id(Some(selected.unwrap_or("")));
}
fn connection(name: &Option<String>, connected: bool) -> String {
    match name {
        None => "Unassigned".into(),
        Some(name) if connected => format!("Connected: {name}"),
        Some(name) => format!("Unavailable: {name}"),
    }
}

fn muted_label(muted: bool) -> &'static str {
    if muted {
        " (muted)"
    } else {
        ""
    }
}
