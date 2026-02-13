// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Frame type for threaded encoding.

/// A frame ready for encoding.
///
/// This type is used for sending frames between threads
/// in the streaming coordinator.
#[derive(Debug, Clone)]
pub struct EncodeFrame {
    /// RGB image data
    pub data: Vec<u8>,

    /// Frame width
    pub width: u32,

    /// Frame height
    pub height: u32,

    /// Frame timestamp (presentation time)
    pub timestamp: u64,
}

impl EncodeFrame {
    /// Create a new encode frame.
    pub fn new(data: Vec<u8>, width: u32, height: u32, timestamp: u64) -> Self {
        Self {
            data,
            width,
            height,
            timestamp,
        }
    }

    /// Get the expected data size for RGB format.
    pub fn rgb_size(&self) -> usize {
        (self.width * self.height * 3) as usize
    }

    /// Validate the frame data.
    pub fn validate(&self) -> bool {
        self.data.len() == self.rgb_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_frame() {
        let data = vec![0u8; 640 * 480 * 3];
        let frame = EncodeFrame::new(data.clone(), 640, 480, 0);

        assert_eq!(frame.width, 640);
        assert_eq!(frame.height, 480);
        assert_eq!(frame.timestamp, 0);
        assert!(frame.validate());
        assert_eq!(frame.rgb_size(), data.len());
    }

    #[test]
    fn test_encode_frame_invalid() {
        let data = vec![0u8; 100]; // Wrong size
        let frame = EncodeFrame::new(data, 640, 480, 0);

        assert!(!frame.validate());
    }
}
