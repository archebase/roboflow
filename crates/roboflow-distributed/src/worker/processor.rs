// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use std::sync::Arc;

use crate::batch::WorkUnit;
use crate::tikv::TikvError;

use super::metrics::ProcessingResult;

#[async_trait::async_trait]
pub trait WorkProcessor: Send + Sync {
    async fn process(&self, work_unit: &WorkUnit) -> Result<ProcessingResult, TikvError>;

    /// Optional hook called after process() to register staging completion.
    ///
    /// This is called by workers when they finish processing work units
    /// that use cloud storage staging. The default implementation does nothing
    /// for backward compatibility with local-only processors.
    async fn on_staging_complete(
        &self,
        _work_unit: &WorkUnit,
        _staging_path: &str,
        _frame_count: u64,
    ) -> Result<(), TikvError> {
        // Default: do nothing (backward compatible)
        Ok(())
    }
}

pub struct MissingWorkProcessor;

#[async_trait::async_trait]
impl WorkProcessor for MissingWorkProcessor {
    async fn process(&self, work_unit: &WorkUnit) -> Result<ProcessingResult, TikvError> {
        Ok(ProcessingResult::Failed {
            error: format!(
                "No WorkProcessor configured for work unit {} in batch {}",
                work_unit.id, work_unit.batch_id
            ),
        })
    }
}

pub type SharedWorkProcessor = Arc<dyn WorkProcessor>;
