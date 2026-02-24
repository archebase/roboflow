// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use std::sync::Arc;

use crate::batch::WorkUnit;
use crate::tikv::TikvError;

use super::coordinator::Coordinator;
use super::metrics::ProcessingResult;

#[async_trait::async_trait]
pub trait WorkProcessor: Send + Sync {
    async fn process(
        &self,
        coordinator: &Coordinator,
        work_unit: &WorkUnit,
    ) -> Result<ProcessingResult, TikvError>;
}

pub struct DirectWorkProcessor;

#[async_trait::async_trait]
impl WorkProcessor for DirectWorkProcessor {
    async fn process(
        &self,
        coordinator: &Coordinator,
        work_unit: &WorkUnit,
    ) -> Result<ProcessingResult, TikvError> {
        coordinator.execute_work_unit_direct(work_unit).await
    }
}

pub type SharedWorkProcessor = Arc<dyn WorkProcessor>;
