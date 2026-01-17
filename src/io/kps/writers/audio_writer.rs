//! Audio writer for Kps v1.2 datasets.
//!
//! Writes audio data to WAV files in the audio/ directory.

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::io::kps::writers::base::{AudioData, KpsWriterError};

/// Audio writer for Kps datasets.
///
/// Writes audio data as WAV files to the audio/ directory.
pub struct AudioWriter {
    /// Output directory path.
    output_dir: PathBuf,

    /// Episode ID.
    episode_id: String,
}

impl AudioWriter {
    /// Create a new audio writer.
    pub fn new(output_dir: impl AsRef<Path>, episode_id: &str) -> Self {
        Self {
            output_dir: output_dir.as_ref().to_path_buf(),
            episode_id: episode_id.to_string(),
        }
    }

    /// Initialize the audio writer (creates audio/ directory).
    pub fn initialize(&mut self) -> Result<(), KpsWriterError> {
        let audio_dir = self.output_dir.join("audio");
        std::fs::create_dir_all(&audio_dir).map_err(|e| KpsWriterError::Io(e))?;

        tracing::info!(
            path = %audio_dir.display(),
            "Initialized audio writer"
        );

        Ok(())
    }

    /// Write audio data to a WAV file.
    ///
    /// # Arguments
    /// * `name` - Base name for the audio file (without extension)
    /// * `data` - Audio data to write
    pub fn write_audio_file(
        &self,
        name: &str,
        data: &AudioData,
    ) -> Result<PathBuf, KpsWriterError> {
        let audio_dir = self.output_dir.join("audio");
        let wav_path = audio_dir.join(format!("{}.wav", name));

        // Ensure directory exists
        std::fs::create_dir_all(&audio_dir).map_err(|e| KpsWriterError::Io(e))?;

        // Write WAV file
        let mut file = File::create(&wav_path).map_err(|e| KpsWriterError::Io(e))?;

        // Write WAV header
        self.write_wav_header(&mut file, data)?;

        // Write audio data
        for &sample in &data.samples {
            let sample_i16 = (sample.clamp(-1.0, 1.0) * i16::MAX as f32) as i16;
            file.write_all(&sample_i16.to_le_bytes())
                .map_err(|e| KpsWriterError::Io(e))?;
        }

        tracing::info!(
            path = %wav_path.display(),
            samples = data.samples.len(),
            sample_rate = data.sample_rate,
            channels = data.channels,
            "Wrote audio file"
        );

        Ok(wav_path)
    }

    /// Write a WAV header.
    fn write_wav_header(&self, file: &mut File, data: &AudioData) -> Result<(), KpsWriterError> {
        let byte_rate = data.sample_rate * data.channels as u32 * 2; // 16-bit = 2 bytes
        let block_align = data.channels as u32 * 2;
        let data_size = data.samples.len() as u32 * 2;
        let file_size = 36 + data_size;

        // RIFF header
        file.write_all(b"RIFF").map_err(|e| KpsWriterError::Io(e))?;
        file.write_all(&file_size.to_le_bytes())
            .map_err(|e| KpsWriterError::Io(e))?;
        file.write_all(b"WAVE").map_err(|e| KpsWriterError::Io(e))?;

        // fmt chunk
        file.write_all(b"fmt ").map_err(|e| KpsWriterError::Io(e))?;
        file.write_all(&16u32.to_le_bytes()) // Chunk size
            .map_err(|e| KpsWriterError::Io(e))?;
        file.write_all(&1u16.to_le_bytes()) // Audio format (1 = PCM)
            .map_err(|e| KpsWriterError::Io(e))?;
        file.write_all(&data.channels.to_le_bytes())
            .map_err(|e| KpsWriterError::Io(e))?;
        file.write_all(&data.sample_rate.to_le_bytes())
            .map_err(|e| KpsWriterError::Io(e))?;
        file.write_all(&byte_rate.to_le_bytes())
            .map_err(|e| KpsWriterError::Io(e))?;
        file.write_all(&block_align.to_le_bytes())
            .map_err(|e| KpsWriterError::Io(e))?;
        file.write_all(&16u16.to_le_bytes()) // Bits per sample
            .map_err(|e| KpsWriterError::Io(e))?;

        // data chunk
        file.write_all(b"data").map_err(|e| KpsWriterError::Io(e))?;
        file.write_all(&data_size.to_le_bytes())
            .map_err(|e| KpsWriterError::Io(e))?;

        Ok(())
    }

    /// Write multiple audio files.
    pub fn write_audio_files(
        &self,
        audio_data: &HashMap<String, AudioData>,
    ) -> Result<Vec<PathBuf>, KpsWriterError> {
        let mut paths = Vec::new();

        for (name, data) in audio_data {
            let path = self.write_audio_file(name, data)?;
            paths.push(path);
        }

        Ok(paths)
    }

    /// Get the audio directory path.
    pub fn audio_dir(&self) -> PathBuf {
        self.output_dir.join("audio")
    }
}

/// Factory for creating audio writers.
pub struct AudioWriterFactory;

impl AudioWriterFactory {
    /// Create a new audio writer.
    pub fn create(output_dir: impl AsRef<Path>, episode_id: &str) -> AudioWriter {
        AudioWriter::new(output_dir, episode_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audio_data_duration() {
        let data = AudioData {
            samples: vec![0.0f32; 48000], // 1 second at 48kHz mono
            sample_rate: 48000,
            channels: 1,
            original_timestamp: 0,
        };

        assert!((data.duration() - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_audio_data_frames() {
        let data = AudioData {
            samples: vec![0.0f32; 96000], // 1 second stereo at 48kHz
            sample_rate: 48000,
            channels: 2,
            original_timestamp: 0,
        };

        assert_eq!(data.frames(), 48000);
    }

    #[test]
    fn test_audio_data_clamping() {
        let data = AudioData {
            samples: vec![-2.0, 0.0, 0.5, 1.0, 2.0],
            sample_rate: 48000,
            channels: 1,
            original_timestamp: 0,
        };

        let writer = AudioWriter {
            output_dir: std::env::temp_dir(),
            episode_id: "test".to_string(),
        };

        // Create temp file for testing
        let temp_dir = std::env::temp_dir();
        let test_path = temp_dir.join("test_audio.wav");

        let mut file = File::create(&test_path).unwrap();
        writer.write_wav_header(&mut file, &data).unwrap();

        for &sample in &data.samples {
            let clamped = (sample.clamp(-1.0, 1.0) * i16::MAX as f32) as i16;
            file.write_all(&clamped.to_le_bytes()).unwrap();
        }

        // Verify file was created
        assert!(test_path.exists());

        // Clean up
        std::fs::remove_file(&test_path).ok();
    }

    #[test]
    fn test_audio_writer_new() {
        let writer = AudioWriter::new("/tmp/output", "episode_001");
        assert_eq!(writer.episode_id, "episode_001");
        assert_eq!(writer.output_dir, PathBuf::from("/tmp/output"));
        assert_eq!(writer.audio_dir(), PathBuf::from("/tmp/output/audio"));
    }
}
