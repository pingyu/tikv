// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::RocksEngine as BaseRocksEngine;
use kvproto::{kvrpcpb::Context, metapb, raft_cmdpb};
use raftstore::coprocessor::CoprocessorHost;

use super::{
    Callback, Engine, Error, ErrorInner, ExtCallback, Modify, Result, RocksEngine, SnapContext,
    WriteData,
};

/// A coprocessor engine is a simple wrapper around of other Engines,
/// but providing the ability of coprocessors,
/// to adapt to features depends on observers, e.g. RawKV API V2.
/// Only the `pre_propose_query` is implemented by now.
#[derive(Clone)]
pub struct CoprocessorEngine<E: Engine> {
    base: E,
    coprocessor_host: CoprocessorHost<E::Local>,
}

impl CoprocessorEngine<RocksEngine> {
    pub fn get_rocksdb(&self) -> BaseRocksEngine {
        self.base.get_rocksdb()
    }
}

pub type CoprocessorRocksEngine = CoprocessorEngine<RocksEngine>;

impl<E: Engine> CoprocessorEngine<E> {
    pub fn from_engine(engine: E) -> Self {
        Self {
            base: engine,
            coprocessor_host: CoprocessorHost::default(),
        }
    }

    pub fn register_observer(&mut self, f: impl FnOnce(&mut CoprocessorHost<E::Local>)) {
        f(&mut self.coprocessor_host);
    }

    /// pre_propose is invoked before propose.
    /// It is used to trigger "pre_propose_query" observers for RawKV API V2.
    fn pre_propose(&self, mut batch: WriteData) -> Result<WriteData> {
        let requests = batch
            .modifies
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        let mut cmd_req = raft_cmdpb::RaftCmdRequest::default();
        cmd_req.set_requests(requests.into());

        let mut region = metapb::Region::default();
        region.set_id(1);
        self.coprocessor_host
            .pre_propose(&region, &mut cmd_req)
            .map_err(|err| Error::from(ErrorInner::Other(box_err!(err))))?;

        batch.modifies = cmd_req
            .take_requests()
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();

        Ok(batch)
    }
}

impl<E: Engine> Engine for CoprocessorEngine<E> {
    type Snap = E::Snap;
    type Local = E::Local;

    fn kv_engine(&self) -> Self::Local {
        self.base.kv_engine()
    }

    fn snapshot_on_kv_engine(&self, start_key: &[u8], end_key: &[u8]) -> Result<Self::Snap> {
        self.base.snapshot_on_kv_engine(start_key, end_key)
    }

    fn modify_on_kv_engine(&self, modifies: Vec<Modify>) -> Result<()> {
        self.base.modify_on_kv_engine(modifies)
    }

    fn async_snapshot(&self, ctx: SnapContext<'_>, cb: Callback<Self::Snap>) -> Result<()> {
        self.base.async_snapshot(ctx, cb)
    }

    fn async_write(&self, ctx: &Context, batch: WriteData, write_cb: Callback<()>) -> Result<()> {
        self.async_write_ext(ctx, batch, write_cb, None, None)
    }

    fn async_write_ext(
        &self,
        ctx: &Context,
        mut batch: WriteData,
        write_cb: Callback<()>,
        proposed_cb: Option<ExtCallback>,
        committed_cb: Option<ExtCallback>,
    ) -> Result<()> {
        if batch.modifies.is_empty() {
            return Err(Error::from(ErrorInner::EmptyRequest));
        }

        batch = self.pre_propose(batch)?;
        self.base
            .async_write_ext(ctx, batch, write_cb, proposed_cb, committed_cb)
    }
}
