use std::path::PathBuf;

use roboflow_core::Result;
use roboflow_executor::{PartitionId, Stage, StageId, Task, TaskContext, TaskResult};

pub struct ConvertStage {
    id: StageId,
    output_dir: PathBuf,
    partition_count: usize,
}

impl ConvertStage {
    pub fn new(id: StageId, output_dir: impl Into<PathBuf>, partition_count: usize) -> Self {
        Self {
            id,
            output_dir: output_dir.into(),
            partition_count,
        }
    }
}

impl Stage for ConvertStage {
    fn id(&self) -> StageId {
        self.id
    }

    fn name(&self) -> &str {
        "convert"
    }

    fn partition_count(&self) -> usize {
        self.partition_count
    }

    fn create_task(&self, partition: PartitionId) -> Box<dyn Task> {
        Box::new(ConvertTask {
            output_dir: self.output_dir.clone(),
            partition,
        })
    }
}

#[allow(dead_code)]
struct ConvertTask {
    output_dir: PathBuf,
    partition: PartitionId,
}

#[async_trait::async_trait]
impl Task for ConvertTask {
    async fn execute(&mut self, _ctx: &TaskContext) -> Result<TaskResult> {
        Ok(TaskResult {
            outputs: vec![],
            metrics: roboflow_executor::TaskMetrics::default(),
            status: roboflow_executor::TaskStatus::Success,
        })
    }
}
