// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use pd_client::Error as PdError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("PD error {0}")]
    Pd(#[from] PdError),
    #[error("Other error {0}")]
    Other(#[from] Box<dyn std::error::Error + Sync + Send>),
}

pub type Result<T> = std::result::Result<T, Error>;
