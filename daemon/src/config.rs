use anyhow::{Context, Result};
use twinsink_core::{config_path, AppConfig};

pub fn load_config() -> Result<AppConfig> {
    let path = config_path();
    if path.exists() {
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read config file at {}", path.display()))?;
        let cfg = AppConfig::from_toml(&content)
            .with_context(|| format!("Failed to parse config file at {}", path.display()))?;
        Ok(cfg)
    } else {
        Ok(AppConfig::default())
    }
}

pub fn save_config(cfg: &AppConfig) -> Result<()> {
    let path = config_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!("Failed to create config directory at {}", parent.display())
        })?;
    }
    let content = cfg
        .to_toml()
        .map_err(|e| anyhow::anyhow!("Failed to serialize config to TOML: {}", e))?;
    std::fs::write(&path, content)
        .with_context(|| format!("Failed to write config file at {}", path.display()))?;
    Ok(())
}
