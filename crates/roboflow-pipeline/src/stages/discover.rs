use std::path::PathBuf;

use roboflow_core::Result;
use roboflow_executor::{PartitionId, Stage, StageId, Task, TaskContext, TaskResult};

pub struct DiscoverStage {
    id: StageId,
    input_dir: PathBuf,
}

impl DiscoverStage {
    pub fn new(id: StageId, input_dir: impl Into<PathBuf>) -> Self {
        Self {
            id,
            input_dir: input_dir.into(),
        }
    }
}

impl Stage for DiscoverStage {
    fn id(&self) -> StageId {
        self.id
    }

    fn name(&self) -> &str {
        "discover"
    }

    fn partition_count(&self) -> usize {
        1
    }

    fn create_task(&self, partition: PartitionId) -> Box<dyn Task> {
        Box::new(DiscoverTask {
            input_dir: self.input_dir.clone(),
            partition,
        })
    }
}

#[allow(dead_code)]
struct DiscoverTask {
    input_dir: PathBuf,
    partition: PartitionId,
}

#[async_trait::async_trait]
impl Task for DiscoverTask {
    async fn execute(&mut self, _ctx: &TaskContext) -> Result<TaskResult> {
        Ok(TaskResult {
            outputs: vec![],
            metrics: roboflow_executor::TaskMetrics::default(),
            status: roboflow_executor::TaskStatus::Success,
        })
    }
}
