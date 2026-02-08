// Decoder stage - wraps robocodec's streaming decoder

use std::thread::{self, JoinHandle};
use std::time::Instant;

use crossbeam_channel::Sender;

use crate::streaming::pipeline::types::{DecodedMessage, PipelineError, PipelineResult};

/// Statistics from the decoder stage.
#[derive(Debug, Clone)]
pub struct DecoderStats {
    /// Total messages decoded
    pub messages_decoded: usize,
    /// Processing time in seconds
    pub duration_sec: f64,
}

/// The decoder stage.
///
/// This stage wraps robocodec's RoboReader.decoded() streaming iterator.
/// No prefetching is needed - RoboReader handles optimized I/O internally.
pub struct DecoderStage {
    /// Input file path
    input_path: std::path::PathBuf,
    /// Output channel for decoded messages
    output_tx: Sender<DecodedMessage>,
}

impl DecoderStage {
    /// Create a new decoder stage.
    pub fn new(input_path: std::path::PathBuf, output_tx: Sender<DecodedMessage>) -> Self {
        Self {
            input_path,
            output_tx,
        }
    }

    /// Spawn the decoder in a thread.
    pub fn spawn(self) -> JoinHandle<PipelineResult<DecoderStats>> {
        thread::spawn(move || {
            let name = "Decoder";
            tracing::debug!(
                input = %self.input_path.display(),
                "{name} starting"
            );

            let start = Instant::now();
            let result = self.run_internal();
            let duration = start.elapsed();

            match &result {
                Ok(stats) => {
                    tracing::debug!(
                        duration_sec = duration.as_secs_f64(),
                        messages = stats.messages_decoded,
                        "{name} completed"
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, "{name} failed");
                }
            }

            result.map(|mut stats| {
                stats.duration_sec = duration.as_secs_f64();
                stats
            })
        })
    }

    fn run_internal(&self) -> PipelineResult<DecoderStats> {
        use robocodec::RoboReader;

        let path_str = self
            .input_path
            .to_str()
            .ok_or_else(|| PipelineError::ExecutionFailed {
                stage: "Decoder".to_string(),
                reason: "Invalid UTF-8 path".to_string(),
            })?;

        // Open robocodec reader - this handles file I/O optimization internally
        let reader = RoboReader::open(path_str).map_err(|e| PipelineError::ExecutionFailed {
            stage: "Decoder".to_string(),
            reason: format!("Failed to open input: {e}"),
        })?;

        let mut messages_decoded = 0usize;

        // Use robocodec's streaming iterator - decoded() returns a lazy iterator
        // Messages are decoded on-demand, not loaded all at once
        // msg.message is HashMap<String, robocodec::CodecValue>
        for msg_result in reader
            .decoded()
            .map_err(|e| PipelineError::ExecutionFailed {
                stage: "Decoder".to_string(),
                reason: format!("Failed to get decoded iterator: {e}"),
            })?
        {
            let msg = msg_result.map_err(|e| PipelineError::ExecutionFailed {
                stage: "Decoder".to_string(),
                reason: format!("Decode error: {e}"),
            })?;

            // Convert TimestampedDecodedMessage to our DecodedMessage
            // msg.message is HashMap<String, CodecValue>, which is what we need
            let decoded = DecodedMessage {
                topic: msg.channel.topic.clone(),
                message_type: msg.channel.message_type.clone(),
                log_time: msg.log_time.unwrap_or(0),
                sequence: msg.sequence,
                // msg.message is already HashMap<String, robocodec::CodecValue>
                // Wrap it in CodecValue::Struct for our DecodedMessage.data
                data: robocodec::CodecValue::Struct(msg.message),
            };

            self.output_tx
                .send(decoded)
                .map_err(|e| PipelineError::ChannelError {
                    from: "Decoder".to_string(),
                    to: "Aligner".to_string(),
                    reason: e.to_string(),
                })?;

            messages_decoded += 1;

            if messages_decoded.is_multiple_of(10000) {
                tracing::debug!(messages = messages_decoded, "Decoder progress");
            }
        }

        Ok(DecoderStats {
            messages_decoded,
            duration_sec: 0.0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decoder_stage_creation() {
        use crossbeam_channel::bounded;
        let (tx, _rx) = bounded(10);
        let stage = DecoderStage::new(std::path::PathBuf::from("test.bag"), tx);
        assert_eq!(stage.input_path, std::path::PathBuf::from("test.bag"));
    }
}
