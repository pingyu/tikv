// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::error;

use error_code::{self, ErrorCode, ErrorCodeExt};

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Pd(err: pd_client::Error) {
            from()
            cause(err)
            display("PdClient {}", err)
        }
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            display("{:?}", err)
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Pd(_) => error_code::causal_ts::PD,
            Error::Other(_) => error_code::UNKNOWN,
        }
    }
}
