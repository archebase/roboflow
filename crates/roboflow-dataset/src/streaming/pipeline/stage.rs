// Pipeline stage trait and common infrastructure

use crossbeam_channel::{Receiver, Sender, bounded};

/// Channel capacity configuration for inter-stage communication.
#[derive(Debug, Clone, Copy)]
pub struct ChannelConfig {
    /// Capacity of message channels
    pub message_capacity: usize,
    /// Capacity of frame channels
    pub frame_capacity: usize,
    /// Capacity of data channels (bytes, large chunks)
    pub data_capacity: usize,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            message_capacity: 10000,
            frame_capacity: 100,
            data_capacity: 16,
        }
    }
}

impl ChannelConfig {
    /// Create with high capacity for high-throughput scenarios
    pub fn high_throughput() -> Self {
        Self {
            message_capacity: 50000,
            frame_capacity: 500,
            data_capacity: 32,
        }
    }

    /// Create with low capacity for memory-constrained scenarios
    pub fn low_memory() -> Self {
        Self {
            message_capacity: 1000,
            frame_capacity: 10,
            data_capacity: 4,
        }
    }

    /// Create bounded channels for inter-stage communication
    pub fn create_channels<T>(&self, capacity: usize) -> (Sender<T>, Receiver<T>) {
        bounded(capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_config_default() {
        let config = ChannelConfig::default();
        assert_eq!(config.message_capacity, 10000);
        assert_eq!(config.frame_capacity, 100);
        assert_eq!(config.data_capacity, 16);
    }

    #[test]
    fn test_channel_config_high_throughput() {
        let config = ChannelConfig::high_throughput();
        assert_eq!(config.message_capacity, 50000);
        assert_eq!(config.frame_capacity, 500);
        assert_eq!(config.data_capacity, 32);
    }

    #[test]
    fn test_channel_config_low_memory() {
        let config = ChannelConfig::low_memory();
        assert_eq!(config.message_capacity, 1000);
        assert_eq!(config.frame_capacity, 10);
        assert_eq!(config.data_capacity, 4);
    }

    #[test]
    fn test_channel_config_create_channels() {
        let config = ChannelConfig::default();
        let (tx, rx) = config.create_channels::<usize>(10);
        assert!(tx.try_send(42).is_ok());
        assert_eq!(rx.recv().unwrap(), 42);
    }
}
