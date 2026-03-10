use roboflow_dataset::formats::lerobot::config::{DatasetBaseConfig, DatasetConfig, LerobotConfig};
use roboflow_executor::{Stage, StageId};
use roboflow_pipeline::stages::{ConvertStage, DiscoverStage, MergeStage};

fn test_lerobot_config() -> LerobotConfig {
    LerobotConfig {
        dataset: DatasetConfig {
            base: DatasetBaseConfig {
                name: "test".to_string(),
                fps: 30,
                robot_type: None,
            },
            env_type: None,
        },
        mappings: vec![],
        video: Default::default(),
        annotation_file: None,
        flushing: Default::default(),
        streaming: Default::default(),
    }
}

#[test]
fn test_discover_stage_name() {
    let stage = DiscoverStage::from_dir(StageId(0), "/input");
    assert_eq!(stage.name(), "discover");
}

#[test]
fn test_discover_stage_id() {
    let stage = DiscoverStage::from_dir(StageId(42), "/input");
    assert_eq!(stage.id().0, 42);
}

#[test]
fn test_discover_stage_partition_count() {
    let stage = DiscoverStage::from_dir(StageId(0), "/input");
    assert_eq!(stage.partition_count(), 1);
}

#[test]
fn test_convert_stage_name() {
    let stage = ConvertStage::new(
        StageId(0),
        "/output",
        vec![
            "a.bag".into(),
            "b.bag".into(),
            "c.bag".into(),
            "d.bag".into(),
        ],
        test_lerobot_config(),
    );
    assert_eq!(stage.name(), "convert");
}

#[test]
fn test_convert_stage_id() {
    let stage = ConvertStage::new(
        StageId(42),
        "/output",
        vec![
            "a.bag".into(),
            "b.bag".into(),
            "c.bag".into(),
            "d.bag".into(),
        ],
        test_lerobot_config(),
    );
    assert_eq!(stage.id().0, 42);
}

#[test]
fn test_convert_stage_partition_count() {
    let stage = ConvertStage::new(
        StageId(0),
        "/output",
        vec![
            "a.bag".into(),
            "b.bag".into(),
            "c.bag".into(),
            "d.bag".into(),
            "e.bag".into(),
            "f.bag".into(),
            "g.bag".into(),
            "h.bag".into(),
        ],
        test_lerobot_config(),
    );
    assert_eq!(stage.partition_count(), 8);
}

#[test]
fn test_merge_stage_name() {
    let stage = MergeStage::new(StageId(0), "/output", vec![]);
    assert_eq!(stage.name(), "merge");
}

#[test]
fn test_merge_stage_id() {
    let stage = MergeStage::new(StageId(42), "/output", vec![]);
    assert_eq!(stage.id().0, 42);
}

#[test]
fn test_merge_stage_partition_count() {
    let stage = MergeStage::new(StageId(0), "/output", vec![]);
    assert_eq!(stage.partition_count(), 1);
}
