// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Conversion between sink types and dataset writer types.
//!
//! The sink layer uses `DatasetFrame` / `ImageData` / `ImageFormat`,
//! while dataset writers use `AlignedFrame` / `dataset::ImageData`.
//! This module bridges the two.

use crate::{DatasetFrame, ImageFormat};
use roboflow_dataset::common::base::AlignedFrame;

/// Convert a `DatasetFrame` (sink type) to an `AlignedFrame` (dataset writer type).
///
/// Mapping:
/// - `frame_index` → direct
/// - `timestamp` (f64 seconds) → `timestamp` (u64 nanoseconds)
/// - `observation_state` → `states["observation.state"]`
/// - `action` → `actions["action"]`
/// - `images` → converted `ImageData` types
/// - `additional_data` → appended to `states`
pub(crate) fn dataset_frame_to_aligned(frame: &DatasetFrame) -> AlignedFrame {
    let timestamp_ns = (frame.timestamp * 1_000_000_000.0) as u64;
    let mut aligned = AlignedFrame::new(frame.frame_index, timestamp_ns);

    // Observation state
    if let Some(ref state) = frame.observation_state {
        aligned.add_state("observation.state".to_string(), state.clone());
    }

    // Action
    if let Some(ref action) = frame.action {
        aligned.add_action("action".to_string(), action.clone());
    }

    // Images
    for (feature_name, img) in &frame.images {
        let is_encoded = matches!(img.format, ImageFormat::Jpeg | ImageFormat::Png);
        let dataset_img = roboflow_dataset::ImageData {
            width: img.width,
            height: img.height,
            data: img.data.clone(),
            original_timestamp: timestamp_ns,
            is_encoded,
            is_depth: false,
        };
        aligned.add_image(feature_name.clone(), dataset_img);
    }

    // Additional data → states
    for (key, values) in &frame.additional_data {
        aligned.add_state(key.clone(), values.clone());
    }

    aligned
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ImageData;

    #[test]
    fn test_basic_conversion() {
        let frame = DatasetFrame::new(5, 0, 1.5)
            .with_observation_state(vec![1.0, 2.0, 3.0])
            .with_action(vec![0.5, 0.6]);

        let aligned = dataset_frame_to_aligned(&frame);

        assert_eq!(aligned.frame_index, 5);
        assert_eq!(aligned.timestamp, 1_500_000_000);
        assert_eq!(
            aligned.states.get("observation.state"),
            Some(&vec![1.0, 2.0, 3.0])
        );
        assert_eq!(aligned.actions.get("action"), Some(&vec![0.5, 0.6]));
    }

    #[test]
    fn test_image_conversion_rgb() {
        let mut frame = DatasetFrame::new(0, 0, 0.0);
        frame.images.insert(
            "observation.camera_0".to_string(),
            ImageData {
                width: 2,
                height: 2,
                data: vec![0u8; 12], // 2x2 RGB
                format: ImageFormat::Rgb8,
            },
        );

        let aligned = dataset_frame_to_aligned(&frame);
        let img = aligned.images.get("observation.camera_0").unwrap();
        assert_eq!(img.width, 2);
        assert_eq!(img.height, 2);
        assert!(!img.is_encoded);
        assert!(!img.is_depth);
    }

    #[test]
    fn test_image_conversion_jpeg() {
        let mut frame = DatasetFrame::new(0, 0, 0.0);
        frame.images.insert(
            "cam".to_string(),
            ImageData {
                width: 640,
                height: 480,
                data: vec![0xFF, 0xD8], // JPEG magic
                format: ImageFormat::Jpeg,
            },
        );

        let aligned = dataset_frame_to_aligned(&frame);
        let img = aligned.images.get("cam").unwrap();
        assert!(img.is_encoded);
    }

    #[test]
    fn test_additional_data_mapping() {
        let mut frame = DatasetFrame::new(0, 0, 0.0);
        frame
            .additional_data
            .insert("observation.gripper".to_string(), vec![0.5]);

        let aligned = dataset_frame_to_aligned(&frame);
        assert_eq!(
            aligned.states.get("observation.gripper"),
            Some(&vec![0.5])
        );
    }

    #[test]
    fn test_empty_frame() {
        let frame = DatasetFrame::new(0, 0, 0.0);
        let aligned = dataset_frame_to_aligned(&frame);
        assert!(aligned.is_empty());
    }
}
