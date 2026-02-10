// SPDX-FileTextCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Frame completion criteria.
//!
//! Defines when a frame is considered "complete" and ready to be emitted.

use std::collections::HashMap;
use std::collections::HashSet;

/// Criteria for determining when a frame is complete.
#[derive(Debug, Clone, Default)]
pub struct FrameCompletionCriteria {
    /// Required features and their minimum counts
    pub features: HashMap<String, usize>,

    /// Minimum completeness ratio (0.0 - 1.0)
    pub min_completeness: f32,
}

impl FrameCompletionCriteria {
    /// Create new completion criteria.
    pub fn new() -> Self {
        Self {
            features: HashMap::new(),
            min_completeness: 0.0,
        }
    }

    /// Add a required feature.
    pub fn require_feature(mut self, feature: impl Into<String>, count: usize) -> Self {
        self.features.insert(feature.into(), count);
        self
    }

    /// Set minimum completeness ratio.
    pub fn with_min_completeness(mut self, ratio: f32) -> Self {
        self.min_completeness = ratio.clamp(0.0, 1.0);
        self
    }

    /// Check if a frame is complete based on received features.
    pub fn is_complete(&self, received_features: &HashSet<String>) -> bool {
        // Check all required features
        for (feature, min_count) in &self.features {
            let count = received_features.iter().filter(|f| **f == *feature).count();
            if count < *min_count {
                return false;
            }
        }

        // Check minimum completeness
        if !self.features.is_empty() && received_features.is_empty() {
            return false;
        }

        // If no specific requirements, any feature is enough
        if self.features.is_empty() && !received_features.is_empty() {
            return true;
        }

        // If no specific requirements AND no received features, not complete
        if self.features.is_empty() && received_features.is_empty() {
            return false;
        }

        // All required features are present
        true
    }

    /// Get the number of required features.
    pub fn required_feature_count(&self) -> usize {
        self.features.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let criteria = FrameCompletionCriteria::new();
        assert_eq!(criteria.features.len(), 0);
        assert_eq!(criteria.min_completeness, 0.0);
    }

    #[test]
    fn test_require_feature() {
        let criteria = FrameCompletionCriteria::new()
            .require_feature("camera_0", 1)
            .require_feature("state", 1);

        assert_eq!(criteria.required_feature_count(), 2);

        let mut received = HashSet::new();
        received.insert("camera_0".to_string());

        // Not complete - missing state
        assert!(!criteria.is_complete(&received));

        received.insert("state".to_string());

        // Complete
        assert!(criteria.is_complete(&received));
    }

    #[test]
    fn test_min_completeness_clamp() {
        let criteria = FrameCompletionCriteria::new().with_min_completeness(1.5);

        assert_eq!(criteria.min_completeness, 1.0);
    }

    #[test]
    fn test_any_feature_sufficient() {
        let criteria = FrameCompletionCriteria::new();

        let mut received = HashSet::new();
        assert!(!criteria.is_complete(&received));

        received.insert("any_feature".to_string());
        assert!(criteria.is_complete(&received));
    }
}
