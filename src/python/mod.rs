// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

//! Python bindings for roboflow.
//!
//! Minimal, clean bindings that expose roboflow's high-level fluent API.

mod fluent;

use pyo3::prelude::*;

use fluent::{
    PyBatchReport, PyCompressionPreset, PyFileResult, PyHyperPipelineReport, PyPipelineReport,
    PyRobocodec, PyTransformBuilder,
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

    Ok(())
}
