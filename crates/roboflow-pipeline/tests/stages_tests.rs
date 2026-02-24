use roboflow_executor::{Stage, StageId, Task, TaskContext};
use roboflow_pipeline::stages::{ConvertStage, DiscoverStage, MergeStage};

#[test]
fn test_discover_stage_name() {
    let stage = DiscoverStage::new(StageId(0), "/input");
    assert_eq!(stage.name(), "discover");
}

#[test]
fn test_discover_stage_id() {
    let stage = DiscoverStage::new(StageId(42), "/input");
    assert_eq!(stage.id().0, 42);
}

#[test]
fn test_discover_stage_partition_count() {
    let stage = DiscoverStage::new(StageId(0), "/input");
    assert_eq!(stage.partition_count(), 1);
}

#[test]
fn test_convert_stage_name() {
    let stage = ConvertStage::new(StageId(0), "/output", 4);
    assert_eq!(stage.name(), "convert");
}

#[test]
fn test_convert_stage_id() {
    let stage = ConvertStage::new(StageId(42), "/output", 4);
    assert_eq!(stage.id().0, 42);
}

#[test]
fn test_convert_stage_partition_count() {
    let stage = ConvertStage::new(StageId(0), "/output", 8);
    assert_eq!(stage.partition_count(), 8);
}

#[test]
fn test_merge_stage_name() {
    let stage = MergeStage::new(StageId(0), "/output");
    assert_eq!(stage.name(), "merge");
}

#[test]
fn test_merge_stage_id() {
    let stage = MergeStage::new(StageId(42), "/output");
    assert_eq!(stage.id().0, 42);
}

#[test]
fn test_merge_stage_partition_count() {
    let stage = MergeStage::new(StageId(0), "/output");
    assert_eq!(stage.partition_count(), 1);
}
