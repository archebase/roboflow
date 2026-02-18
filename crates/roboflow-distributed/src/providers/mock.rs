use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use roboflow_core::TimestampedMessage;
use roboflow_sources::{Source, SourceConfig, SourceError, SourceMetadata, SourceResult};

use super::SourceProvider;

pub struct MockSourceProvider {
    messages: Arc<Mutex<VecDeque<TimestampedMessage>>>,
    metadata: Option<SourceMetadata>,
}

impl MockSourceProvider {
    pub fn new() -> Self {
        Self {
            messages: Arc::new(Mutex::new(VecDeque::new())),
            metadata: None,
        }
    }

    pub fn with_messages(mut self, messages: Vec<TimestampedMessage>) -> Self {
        self.messages = Arc::new(Mutex::new(messages.into()));
        self
    }

    pub fn with_metadata(mut self, metadata: SourceMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

impl Default for MockSourceProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SourceProvider for MockSourceProvider {
    async fn create_source(&self,
        _config: &SourceConfig,
    ) -> SourceResult<Box<dyn Source>> {
        Ok(Box::new(MockSource {
            messages: self.messages.clone(),
            metadata: self.metadata.clone(),
            initialized: false,
        }))
    }
}

pub struct MockSource {
    messages: Arc<Mutex<VecDeque<TimestampedMessage>>>,
    metadata: Option<SourceMetadata>,
    initialized: bool,
}

#[async_trait]
impl Source for MockSource {
    async fn initialize(
        &mut self,
        _config: &SourceConfig,
    ) -> SourceResult<SourceMetadata> {
        self.initialized = true;
        self.metadata.clone().ok_or_else(|| {
            SourceError::InvalidConfig("No metadata configured".to_string())
        })
    }

    async fn read_batch(
        &mut self,
        size: usize,
    ) -> SourceResult<Option<Vec<TimestampedMessage>>> {
        let mut messages = self.messages.lock().unwrap();
        if messages.is_empty() {
            return Ok(None);
        }

        let batch_size = size.min(messages.len());
        let batch: Vec<TimestampedMessage> =
            (0..batch_size).filter_map(|_| messages.pop_front()).collect();

        Ok(Some(batch))
    }

    async fn metadata(&self) -> SourceResult<SourceMetadata> {
        self.metadata.clone().ok_or_else(|| {
            SourceError::InvalidConfig("No metadata configured".to_string())
        })
    }
}

pub struct MockLerobotWriter {
    frames: Arc<Mutex<Vec<MockFrame>>>,
    finalized: Arc<Mutex<bool>>,
}

#[derive(Debug, Clone)]
pub struct MockFrame {
    pub topic: String,
    pub timestamp: u64,
}

impl MockLerobotWriter {
    pub fn new() -> Self {
        Self {
            frames: Arc::new(Mutex::new(Vec::new())),
            finalized: Arc::new(Mutex::new(false)),
        }
    }

    pub fn frames(&self) -> Arc<Mutex<Vec<MockFrame>>> {
        self.frames.clone()
    }

    pub fn is_finalized(&self) -> bool {
        *self.finalized.lock().unwrap()
    }

    pub fn add_frame(&self, topic: String, timestamp: u64) {
        self.frames.lock().unwrap().push(MockFrame { topic, timestamp });
    }
}

impl Default for MockLerobotWriter {
    fn default() -> Self {
        Self::new()
    }
}
