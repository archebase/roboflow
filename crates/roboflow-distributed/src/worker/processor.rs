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
