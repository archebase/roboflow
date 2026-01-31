// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Frame completion criteria for streaming conversion.

use std::collections::{HashMap, HashSet};

use crate::streaming::config::FeatureRequirement;

/// Defines when a frame is considered complete.
///
/// A frame is complete when:
/// 1. All required features have been received, OR
/// 2. The completion window has expired
#[derive(Debug, Clone)]
pub struct FrameCompletionCriteria {
    /// Per-feature requirements
    pub features: HashMap<String, FeatureRequirement>,

    /// Minimum data completeness ratio (0.0 - 1.0)
    pub min_completeness: f32,
}

impl FrameCompletionCriteria {
    /// Create a new completion criteria with no requirements.
    pub fn new() -> Self {
        Self {
            features: HashMap::new(),
            min_completeness: 0.0, // Auto-complete on first data
        }
    }

    /// Add a required feature.
    pub fn require_feature(mut self, feature: impl Into<String>) -> Self {
        self.features
            .insert(feature.into(), FeatureRequirement::Required);
        self
    }

    /// Add an optional feature.
    pub fn optional_feature(mut self, feature: impl Into<String>) -> Self {
        self.features
            .insert(feature.into(), FeatureRequirement::Optional);
        self
    }

    /// Add an "at least N" requirement for multiple features.
    pub fn require_at_least(mut self, features: Vec<String>, min_count: usize) -> Self {
        let req = FeatureRequirement::AtLeast { min_count };
        for feature in features {
            self.features.insert(feature, req);
        }
        self
    }

    /// Set the minimum completeness ratio.
    pub fn with_min_completeness(mut self, ratio: f32) -> Self {
        self.min_completeness = ratio.clamp(0.0, 1.0);
        self
    }

    /// Check if a set of received features meets the completion criteria.
    pub fn is_complete(&self, received_features: &HashSet<String>) -> bool {
        // If no requirements, any data makes it complete
        if self.features.is_empty() {
            return !received_features.is_empty();
        }

        // Check each feature requirement
        for (feature, requirement) in &self.features {
            match requirement {
                FeatureRequirement::Required => {
                    if !received_features.contains(feature) {
                        return false;
                    }
                }
                FeatureRequirement::Optional => {
                    // Optional features don't affect completion
                }
                FeatureRequirement::AtLeast { .. } => {
                    // Track separately for AtLeast requirements
                    // We'll handle these after the loop
                }
            }
        }

        // Check AtLeast requirements by counting satisfied features
        // First, group features by their min_count requirement
        let mut at_least_groups: HashMap<usize, Vec<String>> = HashMap::new();
        for (feature, requirement) in &self.features {
            if let FeatureRequirement::AtLeast { min_count } = requirement {
                at_least_groups
                    .entry(*min_count)
                    .or_default()
                    .push(feature.clone());
            }
        }

        // For each group, check if at least min_count features are received
        for (min_count, features) in at_least_groups {
            let satisfied = features
                .iter()
                .filter(|f| received_features.contains(*f))
                .count();
            // We need at least min_count features from this group
            // But since all features in this group share the same min_count,
            // we check if we have at least min_count features
            let group_size = features.len();
            let required = min_count.min(group_size);
            if satisfied < required {
                return false;
            }
        }

        // Check minimum completeness
        let completeness = self.calculate_completeness(received_features);
        completeness >= self.min_completeness
    }

    /// Calculate the completeness ratio (received / required features).
    fn calculate_completeness(&self, received_features: &HashSet<String>) -> f32 {
        if self.features.is_empty() {
            return 1.0;
        }

        let mut required_count = 0;
        let mut received_count = 0;

        for (feature, requirement) in &self.features {
            match requirement {
                FeatureRequirement::Required => {
                    required_count += 1;
                    if received_features.contains(feature) {
                        received_count += 1;
                    }
                }
                FeatureRequirement::AtLeast { .. } => {
                    // Count these separately
                    required_count += 1;
                    if received_features.contains(feature) {
                        received_count += 1;
                    }
                }
                FeatureRequirement::Optional => {
                    // Optional features don't count toward completeness
                }
            }
        }

        if required_count == 0 {
            1.0
        } else {
            received_count as f32 / required_count as f32
        }
    }
}

impl Default for FrameCompletionCriteria {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_criteria() {
        let criteria = FrameCompletionCriteria::new();
        let mut received = HashSet::new();

        // Empty features = not complete
        assert!(!criteria.is_complete(&received));

        // Any data makes it complete
        received.insert("observation.state".to_string());
        assert!(criteria.is_complete(&received));
    }

    #[test]
    fn test_required_feature() {
        let criteria = FrameCompletionCriteria::new().require_feature("observation.state");

        let mut received = HashSet::new();

        // Missing required feature
        assert!(!criteria.is_complete(&received));

        // Has required feature
        received.insert("observation.state".to_string());
        assert!(criteria.is_complete(&received));
    }

    #[test]
    fn test_optional_feature() {
        let criteria = FrameCompletionCriteria::new()
            .require_feature("observation.state")
            .optional_feature("observation.extra");

        let mut received = HashSet::new();

        // Has required, missing optional
        received.insert("observation.state".to_string());
        assert!(criteria.is_complete(&received));
    }

    #[test]
    fn test_min_completeness() {
        // Test with two required features and min_completeness threshold
        let criteria = FrameCompletionCriteria::new()
            .require_feature("observation.state")
            .require_feature("observation.image")
            .with_min_completeness(0.6);

        let mut received = HashSet::new();

        // Has only 1 of 2 required features (50% complete)
        // With min_completeness 0.6, should not be complete
        received.insert("observation.state".to_string());
        assert!(!criteria.is_complete(&received));

        // Add second required feature - now 100% complete
        received.insert("observation.image".to_string());
        assert!(criteria.is_complete(&received));
    }

    #[test]
    fn test_min_completeness_with_optional() {
        // Optional features don't count toward completeness
        let criteria = FrameCompletionCriteria::new()
            .require_feature("observation.state")
            .optional_feature("observation.extra")
            .with_min_completeness(0.5);

        let mut received = HashSet::new();

        // Has the only required feature (100% complete since optional doesn't count)
        received.insert("observation.state".to_string());
        assert!(criteria.is_complete(&received));

        // Even with min_completeness 0.9, still complete because we have all required features
        let criteria = criteria.with_min_completeness(0.9);
        assert!(criteria.is_complete(&received));
    }
}
