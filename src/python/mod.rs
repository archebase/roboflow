// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Python bindings for roboflow.
//!
//! Minimal, clean bindings that expose roboflow's high-level fluent API.

mod fluent;
mod dataset;

use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;

use fluent::{
    PyBatchReport, PyCompressionPreset, PyFileResult, PyHyperPipelineReport, PyPipelineReport,
    PyRobocodec, PyTransformBuilder,
};

use dataset::{
    PyConversionJob, PyDatasetConfig, PyDatasetConverter, PyDatasetStats, PyKpsConfig,
    PyLerobotConfig, PyProgressUpdate,
};

/// Python module definition
#[pymodule]
fn _roboflow(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    // Main fluent API classes
    m.add_class::<PyRobocodec>()?;
    m.add_class::<PyTransformBuilder>()?;
    m.add_class::<PyCompressionPreset>()?;

    // Result and report classes
    m.add_class::<PyPipelineReport>()?;
    m.add_class::<PyHyperPipelineReport>()?;
    m.add_class::<PyFileResult>()?;
    m.add_class::<PyBatchReport>()?;

    // Dataset API classes
    m.add_class::<PyDatasetConverter>()?;
    m.add_class::<PyLerobotConfig>()?;
    m.add_class::<PyKpsConfig>()?;
    m.add_class::<PyDatasetConfig>()?;
    m.add_class::<PyConversionJob>()?;
    m.add_class::<PyDatasetStats>()?;
    m.add_class::<PyProgressUpdate>()?;
    m.add_function(wrap_pyfunction!(convert, m)?)?;

    Ok(())
}

/// Simple conversion function.
///
/// Direct conversion without creating a converter object.
#[pyfunction]
fn convert(
    py: Python<'_>,
    input_path: String,
    output_dir: String,
    config: &PyDatasetConfig,
) -> PyResult<PyDatasetStats> {
    let converter = match config.format.as_str() {
        "kps" => {
            let py_config = PyKpsConfig::new_rust(
                config.name.clone(),
                config.fps,
                config.robot_type.clone(),
            );
            PyDatasetConverter::kps_rust(output_dir, &py_config)?
        }
        "lerobot" => {
            let py_config = PyLerobotConfig::new_rust(
                config.name.clone(),
                config.fps,
                config.robot_type.clone(),
                None,
            );
            PyDatasetConverter::lerobot_rust(output_dir, &py_config)?
        }
        _ => {
            return Err(PyValueError::new_err(format!(
                "Unknown format: {}",
                config.format
            )));
        }
    };

    converter.convert_rust(py, input_path)
}
