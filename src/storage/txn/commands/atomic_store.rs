// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use api_version::{ApiV2, KvFormat, RawValue};
use causal_ts::CausalTsProvider;
use codec::byte::MemComparableByteCodec;
use engine_traits::raw_ttl::ttl_to_expire_ts;
// #[PerformanceCriticalPath]
use engine_traits::CfName;
use tikv_util::codec::number;

use crate::storage::{
    kv::{Modify, WriteData},
    lock_manager::LockManager,
    txn::{
        commands::{
            Command, CommandExt, ResponsePolicy, TypedCommand, WriteCommand, WriteContext,
            WriteResult,
        },
        Result,
    },
    ProcessResult, Snapshot,
};

command! {
    /// Run Put or Delete for keys which may be changed by `RawCompareAndSwap`.
    RawAtomicStore:
        cmd_ty => (),
        display => "kv::command::atomic_store {:?}", (ctx),
        content => {
            /// The set of mutations to apply.
            cf: CfName,
            mutations: Vec<Modify>,
        }
}

impl CommandExt for RawAtomicStore {
    ctx!();
    tag!(raw_atomic_store);
    gen_lock!(mutations: multiple(|x| x.key()));

    fn write_bytes(&self) -> usize {
        self.mutations.iter().map(|x| x.size()).sum()
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for RawAtomicStore {
    fn process_write(self, _: S, _: WriteContext<'_, L>) -> Result<WriteResult> {
        let rows = self.mutations.len();
        let (mutations, ctx) = (self.mutations, self.ctx);
        let mut to_be_write = WriteData::from_modifies(mutations);
        to_be_write.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr: ProcessResult::Res,
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}

command! {
    RawPut:
        cmd_ty => (),
        display => "kv::command::raw_put {:?}", (ctx),
        content => {
            cf: CfName,
            key: Vec<u8>,
            value: Vec<u8>,
            ttl: u64,
            causal_ts_provider: Option<std::sync::Arc<causal_ts::BatchTsoProvider<pd_client::RpcClient>>>,
        }
}

impl CommandExt for RawPut {
    ctx!();
    tag!(raw_put);
    gen_lock!(empty);

    fn write_bytes(&self) -> usize {
        MemComparableByteCodec::encoded_len(self.key.len()) + number::U64_SIZE + self.value.len()
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for RawPut {
    fn process_write(self, _: S, _: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut ts = None;
        // let mut guard: Option<concurrency_manager::KeyHandleGuard> = None;
        if let Some(ref causal_ts_provider) = self.causal_ts_provider {
            // ts = Some(causal_ts_provider.get_ts().unwrap());
            // let key1 = F::encode_raw_key(&key, ts);
            // guard = CONCURRENCY_MANAGER_LOCK_DURATION_HISTOGRAM.observe_closure_duration(|| {
            //     Some(futures_executor::block_on(
            //         self.concurrency_manager.lock_key(&key1),
            //     ))
            // });
            ts = Some(causal_ts_provider.get_ts().unwrap());
        }

        let (cf, key, value, ttl, ctx) = (self.cf, self.key, self.value, self.ttl, self.ctx);

        let raw_value = RawValue {
            user_value: value,
            expire_ts: ttl_to_expire_ts(ttl),
            is_delete: false,
        };
        let m = Modify::Put(
            cf,
            ApiV2::encode_raw_key_owned(key, ts),
            ApiV2::encode_raw_value_owned(raw_value),
        );
        let mutations = vec![m];
        let rows = mutations.len();

        let mut to_be_write = WriteData::from_modifies(mutations);
        to_be_write.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr: ProcessResult::Res,
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}
