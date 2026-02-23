// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! TiKV key schema for dataset metadata registry.

/// Key prefix for metadata operations.
pub const METADATA_PREFIX: &str = "/roboflow/v1/batch";

/// Key builder for metadata registry.
pub struct MetadataKeys;

impl MetadataKeys {
    /// Task counter for allocating new task indices.
    ///
    /// Key: `/roboflow/v1/batch/{batch_id}/task_counter`
    /// Value: (current_index: u64, version: u64)
    pub fn task_counter(batch_id: &str) -> Vec<u8> {
        format!("{}/{}/task_counter", METADATA_PREFIX, batch_id).into_bytes()
    }

    /// Task entry by hash.
    ///
    /// Key: `/roboflow/v1/batch/{batch_id}/tasks/{task_hash}`
    /// Value: TaskEntry
    pub fn task(batch_id: &str, task_hash: &str) -> Vec<u8> {
        format!("{}/{}/tasks/{}", METADATA_PREFIX, batch_id, task_hash).into_bytes()
    }

    /// Task scan prefix.
    ///
    /// Use with TiKV scan to get all tasks for a batch.
    pub fn task_prefix(batch_id: &str) -> Vec<u8> {
        format!("{}/{}/tasks/", METADATA_PREFIX, batch_id).into_bytes()
    }

    /// Feature specification.
    ///
    /// Key: `/roboflow/v1/batch/{batch_id}/features/{feature_name}`
    /// Value: FeatureSpec
    pub fn feature(batch_id: &str, feature_name: &str) -> Vec<u8> {
        format!("{}/{}/features/{}", METADATA_PREFIX, batch_id, feature_name).into_bytes()
    }

    /// Feature scan prefix.
    pub fn feature_prefix(batch_id: &str) -> Vec<u8> {
        format!("{}/{}/features/", METADATA_PREFIX, batch_id).into_bytes()
    }

    /// Episode metadata.
    ///
    /// Key: `/roboflow/v1/batch/{batch_id}/metadata/episode/{idx:06}`
    /// Value: PartialEpisodeMetadata
    pub fn episode_metadata(batch_id: &str, episode_index: usize) -> Vec<u8> {
        format!(
            "{}/{}/metadata/episode/{:06}",
            METADATA_PREFIX, batch_id, episode_index
        )
        .into_bytes()
    }

    /// Episode metadata scan prefix.
    pub fn episode_metadata_prefix(batch_id: &str) -> Vec<u8> {
        format!("{}/{}/metadata/episode/", METADATA_PREFIX, batch_id).into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_counter_key() {
        let key = MetadataKeys::task_counter("batch-123");
        assert_eq!(
            String::from_utf8(key).unwrap(),
            "/roboflow/v1/batch/batch-123/task_counter"
        );
    }

    #[test]
    fn test_task_key() {
        let key = MetadataKeys::task("batch-123", "abc123");
        assert_eq!(
            String::from_utf8(key).unwrap(),
            "/roboflow/v1/batch/batch-123/tasks/abc123"
        );
    }

    #[test]
    fn test_feature_key() {
        let key = MetadataKeys::feature("batch-123", "observation.state");
        assert_eq!(
            String::from_utf8(key).unwrap(),
            "/roboflow/v1/batch/batch-123/features/observation.state"
        );
    }

    #[test]
    fn test_episode_metadata_key() {
        let key = MetadataKeys::episode_metadata("batch-123", 42);
        assert_eq!(
            String::from_utf8(key).unwrap(),
            "/roboflow/v1/batch/batch-123/metadata/episode/000042"
        );
    }

    #[test]
    fn test_prefixes() {
        let task_prefix = MetadataKeys::task_prefix("batch-123");
        assert!(String::from_utf8(task_prefix).unwrap().ends_with("/tasks/"));

        let feature_prefix = MetadataKeys::feature_prefix("batch-123");
        assert!(
            String::from_utf8(feature_prefix)
                .unwrap()
                .ends_with("/features/")
        );

        let episode_prefix = MetadataKeys::episode_metadata_prefix("batch-123");
        assert!(
            String::from_utf8(episode_prefix)
                .unwrap()
                .ends_with("/metadata/episode/")
        );
    }
}
