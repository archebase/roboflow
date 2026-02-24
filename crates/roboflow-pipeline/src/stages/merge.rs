use std::path::PathBuf;

use roboflow_core::Result;
use roboflow_executor::{PartitionId, Stage, StageId, Task, TaskContext, TaskResult};

pub struct MergeStage {
    id: StageId,
    output_dir: PathBuf,
}

impl MergeStage {
    pub fn new(id: StageId, output_dir: impl Into<PathBuf>) -> Self {
        Self {
            id,
            output_dir: output_dir.into(),
        }
    }
}

impl Stage for MergeStage {
    fn id(&self) -> StageId {
        self.id
    }

    fn name(&self) -> &str {
        "merge"
    }

    fn partition_count(&self) -> usize {
        1
    }

    fn create_task(&self, partition: PartitionId) -> Box<dyn Task> {
        Box::new(MergeTask {
            output_dir: self.output_dir.clone(),
            partition,
        })
    }
}

#[allow(dead_code)]
struct MergeTask {
    output_dir: PathBuf,
    partition: PartitionId,
}

#[async_trait::async_trait]
impl Task for MergeTask {
    async fn execute(&mut self, _ctx: &TaskContext) -> Result<TaskResult> {
        Ok(TaskResult {
            outputs: vec![],
            metrics: roboflow_executor::TaskMetrics::default(),
            status: roboflow_executor::TaskStatus::Success,
        })
    }
}
