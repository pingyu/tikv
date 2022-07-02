// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath
//! Scheduler which schedules the execution of `storage::Command`s.

use std::{marker::PhantomData, sync::Arc};

use api_version::{KvFormat, RawValue};
use engine_traits::CfName;
use tikv_kv::{with_tls_engine, Callback, Engine, Modify, WriteData};
use tikv_util::{sys::SysQuota, yatp_pool::YatpPoolBuilder};

use crate::storage::txn::sched_pool::SchedPool;

#[derive(Clone)]
pub struct RawScheduler<E: Engine, F: KvFormat> {
    inner: Arc<Inner<E, F>>,
}

impl<E: Engine, F: KvFormat> RawScheduler<E, F> {
    pub(in crate::storage) fn new(worker_pool: SchedPool) -> Self {
        let inner = Inner {
            worker_pool,
            _phantom: PhantomData,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub(in crate::storage) fn run_write(&self, batch: WriteData, callback: Callback<()>) {
        self.inner.run_write(batch, callback)
    }
}

struct Inner<E: Engine, F: KvFormat> {
    worker_pool: SchedPool,
    _phantom: PhantomData<F>,
}

impl<E: Engine, F: KvFormat> Inner<E, F> {
    pub(in crate::storage) fn run_write(&self, batch: WriteData, callback: Callback<()>) {
        // TODO: deadline
        // TODO: flow control
        unsafe { with_tls_engine(|engine: &E| engine.async_write_ext()) }
    }
}
