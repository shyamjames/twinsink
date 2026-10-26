use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DeviceState {
    Active,
    Idle,
    Suspended,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AudioDevice {
    pub id: u32,
    pub name: String,
    pub description: String,
    pub state: DeviceState,
    pub volume: f32,
    pub mute: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChannelMode {
    #[default]
    Duplicate,
    Split,
    Mono,
}

impl std::fmt::Display for ChannelMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Duplicate => write!(f, "duplicate"),
            Self::Split => write!(f, "split"),
            Self::Mono => write!(f, "mono"),
        }
    }
}

impl std::str::FromStr for ChannelMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "duplicate" => Ok(Self::Duplicate),
            "split" => Ok(Self::Split),
            "mono" => Ok(Self::Mono),
            other => Err(format!(
                "invalid channel mode: '{}', expected duplicate, split, or mono",
                other
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppConfig {
    pub device_a: Option<String>,
    pub device_b: Option<String>,
    #[serde(default = "default_volume")]
    pub volume_a: f32,
    #[serde(default = "default_volume")]
    pub volume_b: f32,
    #[serde(default)]
    pub mute_a: bool,
    #[serde(default)]
    pub mute_b: bool,
    #[serde(default)]
    pub delay_ms_a: u32,
    #[serde(default)]
    pub delay_ms_b: u32,
    #[serde(default)]
    pub mode: ChannelMode,
}

fn default_volume() -> f32 {
    1.0
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            device_a: None,
            device_b: None,
            volume_a: 1.0,
            volume_b: 1.0,
            mute_a: false,
            mute_b: false,
            delay_ms_a: 0,
            delay_ms_b: 0,
            mode: ChannelMode::Duplicate,
        }
    }
}

impl AppConfig {
    pub fn from_toml(content: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(content)
    }

    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub virtual_sink: String,
    pub device_a: Option<String>,
    pub device_b: Option<String>,
    pub connected_a: bool,
    pub connected_b: bool,
    pub volume_a: f32,
    pub volume_b: f32,
    pub mute_a: bool,
    pub mute_b: bool,
    pub delay_ms_a: u32,
    pub delay_ms_b: u32,
    pub mode: ChannelMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum IpcRequest {
    Status,
    Devices,
    Select {
        device_a: String,
        device_b: String,
    },
    Volume {
        device: String,
        volume: f32,
    },
    Mute {
        device: String,
        #[serde(default)]
        mute: Option<bool>,
    },
    Delay {
        device: String,
        delay_ms: u32,
    },
    Mode {
        mode: ChannelMode,
    },
    Subscribe,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum IpcResponse {
    Ok {
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    Status(DaemonStatus),
    Devices {
        devices: Vec<AudioDevice>,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum DaemonEvent {
    Status(DaemonStatus),
    Devices { devices: Vec<AudioDevice> },
}

pub fn socket_path() -> std::path::PathBuf {
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
        std::path::PathBuf::from(dir).join("twinsink.sock")
    } else {
        std::path::PathBuf::from("/tmp/twinsink.sock")
    }
}

pub fn config_path() -> std::path::PathBuf {
    if let Ok(dir) = std::env::var("XDG_CONFIG_HOME") {
        std::path::PathBuf::from(dir)
            .join("twinsink")
            .join("config.toml")
    } else if let Ok(home) = std::env::var("HOME") {
        std::path::PathBuf::from(home)
            .join(".config")
            .join("twinsink")
            .join("config.toml")
    } else {
        std::path::PathBuf::from("/tmp/twinsink/config.toml")
    }
}
