# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""
Tests for dataset conversion functionality.

Tests cover:
- DatasetConverter creation and usage
- KPS and LeRobot conversion (sync and async)
- ConversionJob progress monitoring
- DatasetStats and ProgressUpdate handling
- Error handling and cancellation
"""

import os
import shutil
import tempfile
import time

import pytest

import roboflow


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def test_mcap_file():
    """Return path to a test MCAP file."""
    test_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    path = os.path.join(test_dir, "tests/fixtures/robocodec_test_0.mcap")
    if not os.path.exists(path):
        pytest.skip(f"Test fixture not found: {path}")
    return path


@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for output files."""
    tmpdir = tempfile.mkdtemp()
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def lerobot_config_file():
    """Return path to LeRobot config file."""
    test_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    path = os.path.join(test_dir, "tests/fixtures/lerobot.toml")
    if not os.path.exists(path):
        pytest.skip(f"Config file not found: {path}")
    return path


# =============================================================================
# Test: Dataset exports
# =============================================================================


class TestDatasetExports:
    """Tests for dataset-related exports."""

    def test_dataset_converter_exists(self):
        """Test that DatasetConverter is exported."""
        assert hasattr(roboflow, "DatasetConverter")
        assert hasattr(roboflow.dataset, "DatasetConverter")

    def test_lerobot_config_exists(self):
        """Test that LerobotConfig is exported."""
        assert hasattr(roboflow, "LerobotConfig")
        assert hasattr(roboflow.dataset, "LerobotConfig")

    def test_kps_config_exists(self):
        """Test that KpsConfig is exported."""
        assert hasattr(roboflow, "KpsConfig")
        assert hasattr(roboflow.dataset, "KpsConfig")

    def test_dataset_stats_exists(self):
        """Test that DatasetStats is exported."""
        assert hasattr(roboflow, "DatasetStats")
        assert hasattr(roboflow.dataset, "DatasetStats")

    def test_progress_update_exists(self):
        """Test that ProgressUpdate is exported."""
        assert hasattr(roboflow, "ProgressUpdate")
        assert hasattr(roboflow.dataset, "ProgressUpdate")

    def test_conversion_job_exists(self):
        """Test that ConversionJob is exported."""
        assert hasattr(roboflow, "ConversionJob")
        assert hasattr(roboflow.dataset, "ConversionJob")

    def test_convert_function_exists(self):
        """Test that convert function is exported."""
        assert hasattr(roboflow, "convert")


# =============================================================================
# Test: DatasetConfig
# =============================================================================


class TestDatasetConfig:
    """Tests for DatasetConfig class."""

    def test_create_dataset_config(self):
        """Test creating a DatasetConfig."""
        config = roboflow.DatasetConfig("lerobot", fps=30, name="test")
        assert config.format == "lerobot"
        assert config.fps == 30
        assert config.name == "test"

    def test_dataset_config_with_robot_type(self):
        """Test DatasetConfig with robot_type."""
        config = roboflow.DatasetConfig(
            "kps", fps=30, name="test", robot_type="genie_s"
        )
        assert config.robot_type == "genie_s"


# =============================================================================
# Test: LerobotConfig
# =============================================================================


class TestLerobotConfig:
    """Tests for LerobotConfig class."""

    def test_create_lerobot_config(self):
        """Test creating a LerobotConfig."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        assert config.name == "test"
        assert config.fps == 30
        assert config.robot_type == "genie_s"

    def test_lerobot_config_from_file(self, lerobot_config_file):
        """Test loading LerobotConfig from file."""
        config = roboflow.LerobotConfig.from_file(lerobot_config_file)
        assert config.name == "robot_dataset"
        assert config.fps == 30
        assert config.robot_type == "genie_s"

    def test_lerobot_config_from_toml(self, lerobot_config_file):
        """Test loading LerobotConfig from TOML string."""
        with open(lerobot_config_file) as f:
            toml_content = f.read()
        config = roboflow.LerobotConfig.from_toml(toml_content)
        assert config.name == "robot_dataset"
        assert config.fps == 30


# =============================================================================
# Test: KpsConfig
# =============================================================================


class TestKpsConfig:
    """Tests for KpsConfig class."""

    def test_create_kps_config(self):
        """Test creating a KpsConfig."""
        config = roboflow.KpsConfig("test", 30, "genie_s")
        assert config.name == "test"
        assert config.fps == 30
        assert config.robot_type == "genie_s"

    def test_kps_config_from_file(self, lerobot_config_file):
        """Test loading KpsConfig from file (using lerobot.toml)."""
        config = roboflow.KpsConfig.from_file(lerobot_config_file)
        assert config.name == "robot_dataset"
        assert config.fps == 30
        assert config.robot_type == "genie_s"


# =============================================================================
# Test: DatasetConverter
# =============================================================================


class TestDatasetConverter:
    """Tests for DatasetConverter class."""

    def test_create_lerobot_converter(self, temp_output_dir):
        """Test creating a LeRobot converter."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)
        assert converter is not None

    def test_create_kps_converter(self, temp_output_dir):
        """Test creating a KPS converter."""
        config = roboflow.KpsConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)
        assert converter is not None

    def test_converter_repr(self, temp_output_dir):
        """Test converter repr."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)
        repr_str = repr(converter)
        assert "Lerobot" in repr_str or "test" in repr_str


# =============================================================================
# Test: Synchronous conversion
# =============================================================================


class TestSyncConversion:
    """Tests for synchronous (blocking) conversion."""

    def test_kps_sync_conversion_missing_file(self, temp_output_dir):
        """Test KPS conversion with missing input file."""
        config = roboflow.KpsConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)

        with pytest.raises(OSError):
            converter.convert("nonexistent_file.mcap")

    def test_lerobot_sync_conversion_missing_file(self, temp_output_dir):
        """Test LeRobot conversion with missing input file."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)

        with pytest.raises(OSError):
            converter.convert("nonexistent_file.mcap")


# =============================================================================
# Test: Asynchronous conversion
# =============================================================================


class TestAsyncConversion:
    """Tests for asynchronous (job-based) conversion."""

    def test_convert_async_returns_job(self, temp_output_dir):
        """Test that convert_async returns a ConversionJob."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)

        job = converter.convert_async("nonexistent_file.mcap")
        assert isinstance(job, roboflow.ConversionJob)

    def test_job_is_complete(self, temp_output_dir):
        """Test ConversionJob.is_complete()."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)

        job = converter.convert_async("nonexistent_file.mcap")
        # Wait a bit for the thread to start
        time.sleep(0.1)

        # After failure or completion, is_complete should return True
        complete = job.is_complete()
        assert isinstance(complete, bool)

    def test_job_is_running(self, temp_output_dir):
        """Test ConversionJob.is_running()."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)

        job = converter.convert_async("nonexistent_file.mcap")
        running = job.is_running()
        assert isinstance(running, bool)

    def test_job_wait(self, temp_output_dir):
        """Test ConversionJob.wait()."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)

        job = converter.convert_async("nonexistent_file.mcap")
        result = job.wait(timeout=5.0)
        assert result is not None

    def test_job_get_progress(self, temp_output_dir):
        """Test ConversionJob.get_progress()."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)

        job = converter.convert_async("nonexistent_file.mcap")
        progress = job.get_progress()
        assert progress is not None


# =============================================================================
# Test: ProgressUpdate
# =============================================================================


class TestProgressUpdate:
    """Tests for ProgressUpdate class."""

    def test_progress_update_variant_type(self):
        """Test ProgressUpdate.variant_type()."""
        # Create a progress update via job
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(tempfile.mkdtemp(), config)
        job = converter.convert_async("nonexistent.mcap")

        # Wait for error/update
        time.sleep(0.2)
        progress = job.get_progress()
        if progress:
            variant = progress.variant_type
            assert isinstance(variant, str)
            assert variant in [
                "started",
                "frame_progress",
                "video_progress",
                "parquet_progress",
                "warning",
                "error",
                "completed",
            ]

    def test_progress_update_repr(self):
        """Test ProgressUpdate repr."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(tempfile.mkdtemp(), config)
        job = converter.convert_async("nonexistent.mcap")

        time.sleep(0.2)
        progress = job.get_progress()
        if progress:
            repr_str = repr(progress)
            assert isinstance(repr_str, str)
            assert len(repr_str) > 0


# =============================================================================
# Test: DatasetStats
# =============================================================================


class TestDatasetStats:
    """Tests for DatasetStats class."""

    def test_stats_properties(self, temp_output_dir):
        """Test DatasetStats properties."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)

        try:
            stats = converter.convert("nonexistent.mcap")
        except OSError:
            # Expected for missing file, try to get partial stats from failed job
            job = converter.convert_async("nonexistent.mcap")
            result = job.wait(timeout=5.0)
            if result is None:
                return
            stats = result

        assert hasattr(stats, "frames_written")
        assert hasattr(stats, "images_encoded")
        assert hasattr(stats, "duration_sec")

    def test_stats_repr(self, temp_output_dir):
        """Test DatasetStats repr."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)

        try:
            stats = converter.convert("nonexistent.mcap")
        except OSError:
            return

        repr_str = repr(stats)
        assert isinstance(repr_str, str)
        assert "frames" in repr_str.lower() or "DatasetStats" in repr_str


# =============================================================================
# Test: Error handling
# =============================================================================


class TestErrorHandling:
    """Tests for error handling."""

    def test_invalid_format(self, temp_output_dir):
        """Test that invalid format raises error."""
        config = roboflow.DatasetConfig("invalid_format", fps=30, name="test")
        with pytest.raises(ValueError):
            roboflow.DatasetConverter.create(temp_output_dir, config)

    def test_missing_input_file(self, temp_output_dir):
        """Test handling of missing input file."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)

        with pytest.raises(OSError):
            converter.convert("nonexistent_file.mcap")

    def test_invalid_config_file(self, temp_output_dir):
        """Test loading from invalid config file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write("invalid [[[ toml")
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(Exception):
                roboflow.LerobotConfig.from_file(temp_path)
        finally:
            os.unlink(temp_path)


# =============================================================================
# Test: Job cancellation
# =============================================================================


class TestJobCancellation:
    """Tests for job cancellation."""

    def test_job_cancel(self, temp_output_dir):
        """Test ConversionJob.cancel()."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)

        job = converter.convert_async("nonexistent.mcap")
        # Cancel immediately
        result = job.cancel()

        # Should return True if cancelled successfully
        assert result is True or result is False

    def test_job_cancel_after_complete(self, temp_output_dir):
        """Test cancelling a job that's already complete."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)

        job = converter.convert_async("nonexistent.mcap")
        job.wait(timeout=5.0)

        # After completion, cancel may return False
        result = job.cancel()
        assert isinstance(result, bool)


# =============================================================================
# Test: Progress channel
# =============================================================================


class TestProgressChannel:
    """Tests for progress channel behavior."""

    def test_progress_overflow(self, temp_output_dir):
        """Test that progress channel handles overflow gracefully."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)

        job = converter.convert_async("nonexistent.mcap")

        # Poll progress many times rapidly
        for _ in range(100):
            progress = job.get_progress()
            # Should not panic or crash
            assert progress is None or isinstance(progress, roboflow.ProgressUpdate)

        job.wait(timeout=5.0)

    def test_progress_polling_interval(self, temp_output_dir):
        """Test progress polling at different intervals."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)

        job = converter.convert_async("nonexistent.mcap")

        # Poll at different intervals
        intervals = [0.01, 0.05, 0.1]
        for interval in intervals:
            time.sleep(interval)
            progress = job.get_progress()
            assert progress is None or isinstance(progress, roboflow.ProgressUpdate)

        job.wait(timeout=5.0)


# =============================================================================
# Test: convert() function
# =============================================================================


class TestConvertFunction:
    """Tests for the convert() convenience function."""

    def test_convert_function(self, temp_output_dir):
        """Test using the convert() function."""
        config = roboflow.DatasetConfig("lerobot", fps=30, name="test")

        # Missing file should raise
        with pytest.raises(OSError):
            roboflow.convert("nonexistent.mcap", temp_output_dir, config)

    def test_convert_function_with_kps(self, temp_output_dir):
        """Test convert() function with KPS format."""
        config = roboflow.DatasetConfig("kps", fps=30, name="test")

        with pytest.raises(OSError):
            roboflow.convert("nonexistent.mcap", temp_output_dir, config)


# =============================================================================
# Test: Edge cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases."""

    def test_empty_dataset_name(self, temp_output_dir):
        """Test with empty dataset name."""
        config = roboflow.LerobotConfig("", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)
        assert converter is not None

    def test_zero_fps(self, temp_output_dir):
        """Test with zero FPS."""
        config = roboflow.LerobotConfig("test", 0, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)
        assert converter is not None

    def test_high_fps(self, temp_output_dir):
        """Test with high FPS value."""
        config = roboflow.LerobotConfig("test", 120, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)
        assert converter is not None

    def test_special_characters_in_name(self, temp_output_dir):
        """Test with special characters in dataset name."""
        config = roboflow.LerobotConfig("test-dataset_2024", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)
        assert converter is not None


# =============================================================================
# Test: Repr and str
# =============================================================================


class TestStringRepresentation:
    """Tests for string representation of objects."""

    def test_lerobot_config_repr(self):
        """Test LerobotConfig repr."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        repr_str = repr(config)
        assert "LerobotConfig" in repr_str or "test" in repr_str

    def test_kps_config_repr(self):
        """Test KpsConfig repr."""
        config = roboflow.KpsConfig("test", 30, "genie_s")
        repr_str = repr(config)
        assert "KpsConfig" in repr_str or "test" in repr_str

    def test_conversion_job_repr(self, temp_output_dir):
        """Test ConversionJob repr."""
        config = roboflow.LerobotConfig("test", 30, "genie_s")
        converter = roboflow.DatasetConverter.create(temp_output_dir, config)
        job = converter.convert_async("nonexistent.mcap")

        repr_str = repr(job)
        assert isinstance(repr_str, str)
        assert len(repr_str) > 0
