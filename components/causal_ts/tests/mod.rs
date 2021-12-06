// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::*;
use test_raftstore::*;

static INIT: Once = Once::new();

pub fn init() {
    INIT.call_once(test_util::setup_for_ci);
}

pub struct TestSuiteBuilder {
    cluster: Option<Cluster<ServerCluster>>,
    memory_quota: Option<usize>,
}

impl TestSuiteBuilder {
    pub fn new() -> TestSuiteBuilder {
        TestSuiteBuilder {
            cluster: None,
            memory_quota: None,
        }
    }

    pub fn cluster(mut self, cluster: Cluster<ServerCluster>) -> TestSuiteBuilder {
        self.cluster = Some(cluster);
        self
    }

    pub fn memory_quota(mut self, memory_quota: usize) -> TestSuiteBuilder {
        self.memory_quota = Some(memory_quota);
        self
    }

    pub fn build(self) -> TestSuite {
        init();
        // let memory_quota = self.memory_quota.unwrap_or(usize::MAX);
        let mut cluster = self.cluster.unwrap();
        // let count = cluster.count;
        let pd_cli = cluster.pd_client.clone();
        // let mut endpoints = HashMap::default();
        // let mut obs = HashMap::default();
        // let mut concurrency_managers = HashMap::default();
        // // Hack! node id are generated from 1..count+1.
        // for id in 1..=count as u64 {
        //     // Create and run cdc endpoints.
        //     let worker = LazyWorker::new(format!("cdc-{}", id));
        //     let mut sim = cluster.sim.wl();

        //     // Register cdc service to gRPC server.
        //     let scheduler = worker.scheduler();
        //     sim.pending_services
        //         .entry(id)
        //         .or_default()
        //         .push(Box::new(move || {
        //             create_change_data(cdc::Service::new(
        //                 scheduler.clone(),
        //                 MemoryQuota::new(memory_quota),
        //             ))
        //         }));
        //     sim.txn_extra_schedulers.insert(
        //         id,
        //         Arc::new(cdc::CdcTxnExtraScheduler::new(worker.scheduler().clone())),
        //     );
        //     let scheduler = worker.scheduler();
        //     let cdc_ob = cdc::CdcObserver::new(scheduler.clone());
        //     obs.insert(id, cdc_ob.clone());
        //     sim.coprocessor_hooks.entry(id).or_default().push(Box::new(
        //         move |host: &mut CoprocessorHost<RocksEngine>| {
        //             cdc_ob.register_to(host);
        //         },
        //     ));
        //     endpoints.insert(id, worker);
        // }

        cluster.run();
        // for (id, worker) in &mut endpoints {
        //     let sim = cluster.sim.wl();
        //     let raft_router = sim.get_server_router(*id);
        //     let cdc_ob = obs.get(id).unwrap().clone();
        //     let cm = sim.get_concurrency_manager(*id);
        //     let env = Arc::new(Environment::new(1));
        //     let cfg = CdcConfig::default();
        //     let mut cdc_endpoint = cdc::Endpoint::new(
        //         DEFAULT_CLUSTER_ID,
        //         &cfg,
        //         pd_cli.clone(),
        //         worker.scheduler(),
        //         raft_router,
        //         cluster.engines[id].kv.clone(),
        //         cdc_ob,
        //         cluster.store_metas[id].clone(),
        //         cm.clone(),
        //         env,
        //         sim.security_mgr.clone(),
        //         MemoryQuota::new(usize::MAX),
        //     );
        //     let mut updated_cfg = cfg.clone();
        //     updated_cfg.min_ts_interval = ReadableDuration::millis(100);
        //     cdc_endpoint.run(Task::ChangeConfig(cfg.diff(&updated_cfg)));
        //     cdc_endpoint.set_max_scan_batch_size(2);
        //     concurrency_managers.insert(*id, cm);
        //     worker.start(cdc_endpoint);
        // }

        TestSuite {
            cluster,
            pd_cli,
            // endpoints,
            // obs,
            // concurrency_managers,
            // env: Arc::new(Environment::new(1)),
            // tikv_cli: HashMap::default(),
            // cdc_cli: HashMap::default(),
        }
    }
}

pub struct TestSuite {
    pub cluster: Cluster<ServerCluster>,
    pub pd_cli: Arc<TestPdClient>,
    // pub endpoints: HashMap<u64, LazyWorker<Task>>,
    // pub obs: HashMap<u64, CdcObserver>,
    // tikv_cli: HashMap<u64, TikvClient>,
    // cdc_cli: HashMap<u64, ChangeDataClient>,
    // concurrency_managers: HashMap<u64, ConcurrencyManager>,

    // env: Arc<Environment>,
}

impl Default for TestSuiteBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TestSuite {
    pub fn new(count: usize) -> TestSuite {
        let mut cluster = new_server_cluster(1, count);
        // Increase the Raft tick interval to make this test case running reliably.
        configure_for_lease_read(&mut cluster, Some(100), None);

        let builder = TestSuiteBuilder::new();
        builder.cluster(cluster).build()
    }

    pub fn stop(mut self) {
        self.cluster.shutdown();
    }
}
