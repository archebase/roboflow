// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Pipeline DAG for stage composition.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::stage::{Stage, StageId};

/// Errors that can occur when building or validating a pipeline.
#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    /// Duplicate stage ID.
    #[error("duplicate stage ID: {0}")]
    DuplicateStage(StageId),

    /// Cyclic dependency detected.
    #[error("cyclic dependency detected")]
    CyclicDependency,

    /// Invalid dependency (references non-existent stage).
    #[error("invalid dependency: stage {0} depends on non-existent stage {1}")]
    InvalidDependency(StageId, StageId),
}

/// A pipeline is a DAG of stages.
///
/// Represents a complete execution plan. Stages are executed in
/// topological order based on their dependencies.
pub struct Pipeline {
    /// All stages in this pipeline.
    stages: HashMap<StageId, Arc<dyn Stage>>,

    /// Stage dependency graph (stage -> dependencies).
    dependencies: HashMap<StageId, Vec<StageId>>,

    /// Cached topological order.
    topological_order: Vec<StageId>,
}

impl Pipeline {
    /// Create a new empty pipeline.
    pub fn empty() -> Self {
        Self {
            stages: HashMap::new(),
            dependencies: HashMap::new(),
            topological_order: Vec::new(),
        }
    }

    /// Get all stages in the pipeline.
    pub fn stages(&self) -> &HashMap<StageId, Arc<dyn Stage>> {
        &self.stages
    }

    /// Get a specific stage by ID.
    pub fn get_stage(&self, id: StageId) -> Option<&Arc<dyn Stage>> {
        self.stages.get(&id)
    }

    /// Get stages in topological order.
    pub fn topological_order(&self) -> &[StageId] {
        &self.topological_order
    }

    /// Get ready stages (all dependencies satisfied).
    pub fn ready_stages(&self, completed: &HashSet<StageId>) -> Vec<StageId> {
        self.stages
            .keys()
            .filter(|stage_id| {
                // Stage is ready if not already completed
                if completed.contains(stage_id) {
                    return false;
                }
                // And all dependencies are completed
                self.dependencies
                    .get(stage_id)
                    .map(|deps| deps.iter().all(|dep| completed.contains(dep)))
                    .unwrap_or(true)
            })
            .copied()
            .collect()
    }

    /// Compute topological order using Kahn's algorithm.
    fn compute_topological_order(
        stages: &HashMap<StageId, Arc<dyn Stage>>,
        dependencies: &HashMap<StageId, Vec<StageId>>,
    ) -> Result<Vec<StageId>, PipelineError> {
        let mut in_degree: HashMap<StageId, usize> = stages.keys().map(|&id| (id, 0)).collect();

        // Count incoming edges
        for (stage_id, deps) in dependencies {
            for dep in deps {
                if stages.contains_key(dep) {
                    *in_degree.get_mut(stage_id).unwrap() += 1;
                }
            }
        }

        let mut queue: Vec<StageId> = in_degree
            .iter()
            .filter(|(_, count)| **count == 0)
            .map(|(&id, _)| id)
            .collect();

        let mut result = Vec::with_capacity(stages.len());

        while let Some(stage_id) = queue.pop() {
            result.push(stage_id);

            // Find stages that depend on this one
            for (id, deps) in dependencies {
                if deps.contains(&stage_id) {
                    let count = in_degree.get_mut(id).unwrap();
                    *count -= 1;
                    if *count == 0 {
                        queue.push(*id);
                    }
                }
            }
        }

        if result.len() != stages.len() {
            return Err(PipelineError::CyclicDependency);
        }

        Ok(result)
    }
}

/// Builder for constructing pipelines.
pub struct PipelineBuilder {
    stages: Vec<Arc<dyn Stage>>,
    dependencies: Vec<(StageId, StageId)>,
}

impl PipelineBuilder {
    /// Create a new pipeline builder.
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            dependencies: Vec::new(),
        }
    }

    /// Add a stage to the pipeline.
    pub fn stage(mut self, stage: Arc<dyn Stage>) -> Self {
        self.stages.push(stage);
        self
    }

    /// Add a dependency: `dependent` depends on `dependency`.
    ///
    /// The `dependent` stage will not start until `dependency` completes.
    pub fn dependency(mut self, dependent: StageId, dependency: StageId) -> Self {
        self.dependencies.push((dependent, dependency));
        self
    }

    /// Build the pipeline.
    ///
    /// Validates the pipeline and computes topological order.
    pub fn build(self) -> Result<Pipeline, PipelineError> {
        let mut stage_map = HashMap::with_capacity(self.stages.len());

        // Check for duplicate stage IDs
        for stage in &self.stages {
            let id = stage.id();
            if stage_map.contains_key(&id) {
                return Err(PipelineError::DuplicateStage(id));
            }
            stage_map.insert(id, Arc::clone(stage));
        }

        // Build dependency map
        let mut dep_map: HashMap<StageId, Vec<StageId>> = HashMap::new();
        for (dependent, dependency) in &self.dependencies {
            // Check that both stages exist
            if !stage_map.contains_key(dependent) {
                return Err(PipelineError::InvalidDependency(*dependent, *dependency));
            }
            if !stage_map.contains_key(dependency) {
                return Err(PipelineError::InvalidDependency(*dependent, *dependency));
            }

            dep_map.entry(*dependent).or_default().push(*dependency);
        }

        // Compute topological order
        let order = Pipeline::compute_topological_order(&stage_map, &dep_map)?;

        Ok(Pipeline {
            stages: stage_map,
            dependencies: dep_map,
            topological_order: order,
        })
    }
}

impl Default for PipelineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stage::{PartitionId, Stage};
    use crate::task::{Task, TaskContext, TaskResult};

    struct MockStage {
        id: StageId,
        name: String,
        deps: Vec<StageId>,
    }

    impl Stage for MockStage {
        fn id(&self) -> StageId {
            self.id
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn partition_count(&self) -> usize {
            1
        }

        fn create_task(&self, _partition: PartitionId) -> Box<dyn Task> {
            unimplemented!()
        }

        fn dependencies(&self) -> Vec<StageId> {
            self.deps.clone()
        }
    }

    #[test]
    fn test_empty_pipeline() {
        let pipeline = Pipeline::empty();
        assert!(pipeline.stages().is_empty());
        assert!(pipeline.topological_order().is_empty());
    }

    #[test]
    fn test_simple_pipeline() {
        let stage0 = Arc::new(MockStage {
            id: StageId(0),
            name: "stage0".to_string(),
            deps: vec![],
        });

        let pipeline = PipelineBuilder::new().stage(stage0).build().unwrap();

        assert_eq!(pipeline.stages().len(), 1);
        assert_eq!(pipeline.topological_order().len(), 1);
    }

    #[test]
    fn test_pipeline_with_dependencies() {
        let stage0 = Arc::new(MockStage {
            id: StageId(0),
            name: "stage0".to_string(),
            deps: vec![],
        });

        let stage1 = Arc::new(MockStage {
            id: StageId(1),
            name: "stage1".to_string(),
            deps: vec![StageId(0)],
        });

        let pipeline = PipelineBuilder::new()
            .stage(stage0)
            .stage(stage1)
            .dependency(StageId(1), StageId(0))
            .build()
            .unwrap();

        assert_eq!(pipeline.topological_order(), &[StageId(0), StageId(1)]);
    }

    #[test]
    fn test_duplicate_stage_error() {
        let stage = Arc::new(MockStage {
            id: StageId(0),
            name: "stage".to_string(),
            deps: vec![],
        });

        let result = PipelineBuilder::new()
            .stage(Arc::clone(&stage))
            .stage(stage)
            .build();

        assert!(matches!(result, Err(PipelineError::DuplicateStage(_))));
    }

    #[test]
    fn test_cyclic_dependency_error() {
        let stage0 = Arc::new(MockStage {
            id: StageId(0),
            name: "stage0".to_string(),
            deps: vec![],
        });

        let stage1 = Arc::new(MockStage {
            id: StageId(1),
            name: "stage1".to_string(),
            deps: vec![],
        });

        let result = PipelineBuilder::new()
            .stage(stage0)
            .stage(stage1)
            .dependency(StageId(0), StageId(1))
            .dependency(StageId(1), StageId(0))
            .build();

        assert!(matches!(result, Err(PipelineError::CyclicDependency)));
    }
}
