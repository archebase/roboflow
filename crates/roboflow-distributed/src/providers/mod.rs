use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use roboflow_core::{Result, RoboflowError};
use roboflow_pipeline::formats::lerobot::LerobotConfig;
use roboflow_pipeline::sources::{Source, SourceConfig, SourceResult};

pub mod mock;

#[async_trait]
pub trait ConfigProvider: Send + Sync + 'static {
    async fn load_config(&self, config_hash: &str) -> Result<LerobotConfig>;
}

#[async_trait]
pub trait SourceProvider: Send + Sync + 'static {
    async fn create_source(&self, config: &SourceConfig) -> SourceResult<Box<dyn Source>>;
}

pub struct ProductionSourceProvider;

impl ProductionSourceProvider {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ProductionSourceProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SourceProvider for ProductionSourceProvider {
    async fn create_source(&self, config: &SourceConfig) -> SourceResult<Box<dyn Source>> {
        roboflow_pipeline::sources::create_source(config)
    }
}

pub struct TikvConfigProvider {
    tikv: Arc<crate::tikv::client::TikvClient>,
}

impl TikvConfigProvider {
    pub fn new(tikv: Arc<crate::tikv::client::TikvClient>) -> Self {
        Self { tikv }
    }
}

#[async_trait]
impl ConfigProvider for TikvConfigProvider {
    async fn load_config(&self, config_hash: &str) -> Result<LerobotConfig> {
        match self
            .tikv
            .get_config(config_hash)
            .await
            .map_err(|e| RoboflowError::other(format!("TiKV error: {}", e)))?
        {
            Some(record) => LerobotConfig::from_toml(&record.content)
                .map_err(|e| RoboflowError::other(format!("TOML parse error: {}", e))),
            None => Err(RoboflowError::other(format!(
                "Config '{}' not found",
                config_hash
            ))),
        }
    }
}

pub struct InMemoryConfigProvider {
    configs: HashMap<String, LerobotConfig>,
}

impl InMemoryConfigProvider {
    pub fn new() -> Self {
        Self {
            configs: HashMap::new(),
        }
    }

    pub fn with_config(mut self, hash: impl Into<String>, config: LerobotConfig) -> Self {
        self.configs.insert(hash.into(), config);
        self
    }
}

impl Default for InMemoryConfigProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ConfigProvider for InMemoryConfigProvider {
    async fn load_config(&self, config_hash: &str) -> Result<LerobotConfig> {
        self.configs
            .get(config_hash)
            .cloned()
            .ok_or_else(|| RoboflowError::other(format!("Config '{}' not found", config_hash)))
    }
}

pub struct ProviderFactory;

impl ProviderFactory {
    pub fn production(
        tikv: Arc<crate::tikv::client::TikvClient>,
    ) -> (ProductionSourceProvider, TikvConfigProvider) {
        (
            ProductionSourceProvider::new(),
            TikvConfigProvider::new(tikv),
        )
    }

    pub fn test() -> (mock::MockSourceProvider, InMemoryConfigProvider) {
        (
            mock::MockSourceProvider::new(),
            InMemoryConfigProvider::new(),
        )
    }
}
