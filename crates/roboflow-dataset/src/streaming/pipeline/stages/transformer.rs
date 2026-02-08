// Feature transformer stage - apply topic to feature mappings

use std::thread::{self, JoinHandle};
use std::time::Instant;

use crossbeam_channel::{Receiver, Sender};

use crate::streaming::pipeline::types::{
    DatasetFrame, PipelineError, PipelineResult, TransformableFrame,
};

/// Statistics from the feature transformer stage.
#[derive(Debug, Clone)]
pub struct TransformerStats {
    /// Frames processed
    pub frames_processed: usize,
    /// Frames produced
    pub frames_produced: usize,
    /// Images extracted
    pub images_extracted: usize,
    /// States extracted
    pub states_extracted: usize,
    /// Processing time in seconds
    pub duration_sec: f64,
}

/// The feature transformer stage.
///
/// Applies topic to feature mappings and extracts structured data.
pub struct FeatureTransformerStage {
    /// Episode index
    episode_index: usize,
    /// Input receiver
    input_rx: Receiver<TransformableFrame>,
    /// Output sender
    output_tx: Sender<DatasetFrame>,
}

impl FeatureTransformerStage {
    /// Create a new feature transformer stage.
    pub fn new(
        episode_index: usize,
        input_rx: Receiver<TransformableFrame>,
        output_tx: Sender<DatasetFrame>,
    ) -> Self {
        Self {
            episode_index,
            input_rx,
            output_tx,
        }
    }

    /// Spawn the transformer in a thread.
    pub fn spawn(
        self,
    ) -> JoinHandle<PipelineResult<(TransformerStats, crate::streaming::pipeline::StageStats)>>
    {
        thread::spawn(move || {
            let name = "FeatureTransformer";
            tracing::debug!("{name} starting");

            let start = Instant::now();
            let result = self.run_internal();
            let duration = start.elapsed();

            match &result {
                Ok((transformer_stats, _stage_stats)) => {
                    tracing::debug!(
                        duration_sec = duration.as_secs_f64(),
                        frames = transformer_stats.frames_processed,
                        images = transformer_stats.images_extracted,
                        states = transformer_stats.states_extracted,
                        "{name} completed"
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, "{name} failed");
                }
            }

            result
        })
    }

    fn run_internal(
        &self,
    ) -> PipelineResult<(TransformerStats, crate::streaming::pipeline::StageStats)> {
        let mut frames_processed = 0usize;
        let mut frames_produced = 0usize;
        let mut images_extracted = 0usize;
        let mut states_extracted = 0usize;

        while let Ok(transformable) = self.input_rx.recv() {
            frames_processed += 1;

            // Convert AlignedFrame to DatasetFrame
            let dataset_frame = DatasetFrame::from_aligned(
                transformable.frame_index,
                self.episode_index,
                transformable.timestamp,
                transformable.aligned_data,
            );

            images_extracted += dataset_frame.images.len();
            if dataset_frame.observation_state.is_some() {
                states_extracted += 1;
            }

            self.output_tx
                .send(dataset_frame)
                .map_err(|e| PipelineError::ChannelError {
                    from: "Transformer".to_string(),
                    to: "Writer".to_string(),
                    reason: e.to_string(),
                })?;

            frames_produced += 1;

            if frames_processed.is_multiple_of(1000) {
                tracing::debug!(
                    frames = frames_processed,
                    images = images_extracted,
                    "Transformer progress"
                );
            }
        }

        Ok((
            TransformerStats {
                frames_processed,
                frames_produced,
                images_extracted,
                states_extracted,
                duration_sec: 0.0,
            },
            crate::streaming::pipeline::StageStats {
                stage: "FeatureTransformer".to_string(),
                items_processed: frames_processed,
                items_produced: frames_produced,
                duration_sec: 0.0,
                peak_memory_mb: None,
                metrics: [
                    (
                        "images_extracted".to_string(),
                        serde_json::json!(images_extracted),
                    ),
                    (
                        "states_extracted".to_string(),
                        serde_json::json!(states_extracted),
                    ),
                ]
                .into_iter()
                .collect(),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transformer_stage_creation() {
        use crossbeam_channel::bounded;

        let (_tx, rx) = bounded(10);
        let (tx, _rx) = bounded(10);
        let stage = FeatureTransformerStage::new(0, rx, tx);
        // Just verify it compiles
        assert_eq!(stage.episode_index, 0);
    }
}
