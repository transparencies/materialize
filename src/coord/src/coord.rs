// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coordination of installed views, available timestamps, compacted timestamps, and transactions.
//!
//! The command coordinator maintains a view of the installed
//! views, and for each tracks the frontier of available times
//! ([`upper`](arrangement_state::Frontiers::upper)) and the frontier
//! of compacted times ([`since`](arrangement_state::Frontiers::since)).
//! The upper frontier describes times that may not return immediately, as any
//! timestamps in advance of the frontier are still open. The since frontier
//! constrains those times for which the maintained view will be correct,
//! as any timestamps in advance of the frontier must accumulate to the same
//! value as would an un-compacted trace. The since frontier cannot be directly
//! mutated, but instead can have multiple handles to it which forward changes
//! from an internal MutableAntichain to the since.
//!
//! The [`Coordinator`] tracks various compaction frontiers
//! so that indexes, compaction, and transactions can work
//! together. [`determine_timestamp()`](Coordinator::determine_timestamp)
//! returns the least valid since of its sources. Any new transactions
//! should thus always be >= the current compaction frontier
//! and so should never change the frontier when being added to
//! [`txn_reads`](Coordinator::txn_reads). The compaction frontier may
//! change when a transaction ends (if it was the oldest transaction and
//! the index's since was advanced after the transaction started) or when
//! [`update_upper()`](Coordinator::update_upper) is run (if there are no in
//! progress transactions before the new since). When it does, it is added to
//! [`since_updates`](Coordinator::since_updates) and will be processed during
//! the next [`maintenance()`](Coordinator::maintenance) call.

use std::cell::RefCell;
use std::cmp;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context};
use chrono::{DateTime, Utc};
use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use futures::future::{self};
use futures::stream::{self, StreamExt};
use lazy_static::lazy_static;
use timely::communication::WorkerGuards;
use timely::order::PartialOrder;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, ChangeBatch, Timestamp as _};
use tokio::runtime::Handle as TokioHandle;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;

use build_info::{BuildInfo, DUMMY_BUILD_INFO};
use dataflow::{TimestampBindingFeedback, WorkerFeedback};
use dataflow_types::logging::LoggingConfig as DataflowLoggingConfig;
use dataflow_types::TimelineId;
use dataflow_types::{
    DataflowDesc, ExternalSourceConnector, IndexDesc, PeekResponse, PostgresSourceConnector,
    SinkConnector, SourceConnector, TailResponse, TimestampSourceUpdate, Update,
};
use expr::{GlobalId, Id, MirRelationExpr, OptimizedMirRelationExpr};
use ore::metrics::MetricsRegistry;
use ore::now::{system_time, to_datetime, EpochMillis, NowFn};
use ore::retry::Retry;
use ore::thread::{JoinHandleExt as _, JoinOnDropHandle};
use repr::{Datum, Diff, Row, Timestamp};
use sql::ast::Raw;
use sql::plan::{MutationKind, Params, PeekWhen, Plan};
use transform::Optimizer;

use self::arrangement_state::{ArrangementFrontiers, Frontiers, SinkWrites};
use self::prometheus::Scraper;
use crate::catalog::builtin::{BUILTINS, MZ_VIEW_FOREIGN_KEYS, MZ_VIEW_KEYS};
use crate::catalog::{self, BuiltinTableUpdate, Catalog, CatalogItem, SinkConnectorState};
use crate::client::{Client, Handle};
use crate::command::{Cancelled, Command, ExecuteResponse};
use crate::coord::antichain::AntichainToken;
use crate::error::CoordError;
use crate::persistcfg::PersistConfig;
use crate::session::Session;
use crate::sink_connector;
use crate::timestamp::{TimestampMessage, Timestamper};
use crate::util::ClientTransmitter;

mod antichain;
pub(crate) mod arrangement_state;
mod dataflow_builder;
mod prometheus;

#[derive(Debug)]
pub enum Message {
    Command(Command),
    Worker(dataflow::Response),
    AdvanceSourceTimestamp(AdvanceSourceTimestamp),
    StatementReady(StatementReady),
    SinkConnectorReady(SinkConnectorReady),
    ScrapeMetrics,
    SendDiffs(SendDiffs),
    WriteLockGrant(tokio::sync::OwnedMutexGuard<()>),
    Shutdown,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct SendDiffs {
    pub session: Session,
    #[derivative(Debug = "ignore")]
    pub tx: ClientTransmitter<ExecuteResponse>,
    pub id: GlobalId,
    pub diffs: Result<Vec<(Row, Diff)>, CoordError>,
    pub kind: MutationKind,
}

#[derive(Debug)]
pub struct AdvanceSourceTimestamp {
    pub id: GlobalId,
    pub update: TimestampSourceUpdate,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct StatementReady {
    pub session: Session,
    #[derivative(Debug = "ignore")]
    pub tx: ClientTransmitter<ExecuteResponse>,
    pub result: Result<sql::ast::Statement<Raw>, CoordError>,
    pub params: Params,
}

/// This is the struct meant to be paired with [`Message::WriteLockGrant`], but
/// could theoretically be used to queue any deferred plan.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct DeferredPlan {
    #[derivative(Debug = "ignore")]
    pub tx: ClientTransmitter<ExecuteResponse>,
    pub session: Session,
    pub plan: Plan,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct SinkConnectorReady {
    pub session: Session,
    #[derivative(Debug = "ignore")]
    pub tx: ClientTransmitter<ExecuteResponse>,
    pub id: GlobalId,
    pub oid: u32,
    pub result: Result<SinkConnector, CoordError>,
}

#[derive(Debug)]
pub struct TimestampedUpdate {
    pub updates: Vec<BuiltinTableUpdate>,
    pub timestamp_offset: u64,
}

/// Configures dataflow worker logging.
#[derive(Clone, Debug)]
pub struct LoggingConfig {
    pub granularity: Duration,
    pub log_logging: bool,
    pub retain_readings_for: Duration,
    pub metrics_scraping_interval: Option<Duration>,
}

/// Configures a coordinator.
pub struct Config<'a> {
    pub workers: usize,
    pub timely_worker: timely::WorkerConfig,
    pub symbiosis_url: Option<&'a str>,
    pub logging: Option<LoggingConfig>,
    pub data_directory: &'a Path,
    pub timestamp_frequency: Duration,
    pub logical_compaction_window: Option<Duration>,
    pub experimental_mode: bool,
    pub disable_user_indexes: bool,
    pub safe_mode: bool,
    pub build_info: &'static BuildInfo,
    pub metrics_registry: MetricsRegistry,
    /// Persistence subsystem configuration.
    pub persist: PersistConfig,
}

/// Glues the external world to the Timely workers.
pub struct Coordinator {
    worker_guards: WorkerGuards<()>,
    worker_txs: Vec<crossbeam_channel::Sender<dataflow::Command>>,
    /// Optimizer instance for logical optimization of views.
    pub(crate) view_optimizer: Optimizer,
    pub(crate) catalog: Catalog,
    pub(crate) symbiosis: Option<symbiosis::Postgres>,
    /// Maps (global Id of arrangement) -> (frontier information). This tracks the
    /// `upper` and computed `since` of the indexes. The `since` is the time at
    /// which we are willing to compact up to. `determine_timestamp()` uses this as
    /// part of its heuristic when determining a viable timestamp for queries.
    pub(crate) indexes: ArrangementFrontiers<Timestamp>,
    /// Map of frontier information for sources
    sources: ArrangementFrontiers<Timestamp>,
    /// Delta from leading edge of an arrangement from which we allow compaction.
    pub(crate) logical_compaction_window_ms: Option<Timestamp>,
    /// Whether base sources are enabled.
    logging_enabled: bool,
    /// Channel to manange internal commands from the coordinator to itself.
    pub(crate) internal_cmd_tx: mpsc::UnboundedSender<Message>,
    /// Channel to communicate source status updates to the timestamper thread.
    pub(crate) ts_tx: std::sync::mpsc::Sender<TimestampMessage>,
    pub(crate) metric_scraper: Scraper,
    /// The timestamp that all local inputs have been advanced up to.
    closed_up_to: Timestamp,
    /// Whether we need to advance local inputs (i.e., did someone observe a timestamp).
    // TODO(justin): this is a hack, and does not work right with TAIL.
    need_advance: bool,
    transient_id_counter: u64,
    /// A map from connection ID to metadata about that connection for all
    /// active connections.
    pub(crate) active_conns: HashMap<u32, ConnMeta>,
    now: NowFn,

    /// Holds pending compaction messages to be sent to the dataflow workers. When
    /// `since_handles` are advanced or `txn_reads` are dropped, this can advance.
    since_updates: Rc<RefCell<HashMap<GlobalId, Antichain<Timestamp>>>>,
    /// Holds handles to ids that are advanced by update_upper.
    pub(crate) since_handles: HashMap<GlobalId, AntichainToken<Timestamp>>,
    /// Tracks active read transactions so that we don't compact any indexes beyond
    /// an in-progress transaction.
    // TODO(mjibson): Should this live on a Session?
    pub(crate) txn_reads: HashMap<u32, TxnReads>,
    /// Tracks write frontiers for active exactly-once sinks.
    pub(crate) sink_writes: HashMap<GlobalId, SinkWrites<Timestamp>>,

    /// A map from pending peeks to the queue into which responses are sent, and
    /// the IDs of workers who have responded.
    pub(crate) pending_peeks: HashMap<u32, (mpsc::UnboundedSender<PeekResponse>, HashSet<usize>)>,
    /// A map from pending tails to the queue into which responses are sent.
    ///
    /// The responses have the form `Vec<Row>` but should perhaps become `TailResponse`.
    pub(crate) pending_tails: HashMap<GlobalId, mpsc::UnboundedSender<Vec<Row>>>,

    /// Serializes accesses to write critical sections.
    pub(crate) write_lock: Arc<tokio::sync::Mutex<()>>,
    /// Holds plans deferred due to write lock.
    pub(crate) write_lock_wait_group: VecDeque<DeferredPlan>,

    /// Tracks timestamps per timeline to provide linearizability
    /// guarantees.
    timelines: HashMap<TimelineId, Timeline>,
}

/// A Timeline provides linearizability to callers by enforcing relationships
/// between reads and writes.
///
/// If read and write timestamps are got in any order, the observed data will
/// respect that order. Each read will see all prior writes, and no subsequent
/// writes. A read at a timestamp can observe writes at that timestamp, so
/// writes after reads will forcibly advance the timestamp.
struct Timeline {
    /// The timeline's current timestamp.
    ts: Timestamp,
    /// Whether the most recent operation was a read.
    last_op_was_read: bool,
}

impl Timeline {
    // Returns a new `Timeline` whose first read and write will be at `ts`.
    fn new(ts: Timestamp) -> Self {
        Self {
            ts,
            last_op_was_read: false,
        }
    }

    /// Assign a timestamp for a read.
    fn get_read_ts(&mut self) -> Timestamp {
        self.last_op_was_read = true;
        self.ts
    }

    /// Assign a timestamp for a write. Writes following reads must ensure that
    /// they are assigned a strictly larger timestamp to ensure they are not
    /// visible to any real-time earlier reads.
    fn get_write_ts(&mut self) -> Timestamp {
        if self.last_op_was_read {
            self.last_op_was_read = false;
            self.ts += 1;
        }
        self.ts
    }

    /// Advances to writes at `now` if it has not yet occurred; if reads at `now`
    /// have already occurred, or the timeline timestamp already passed `now`, this
    /// call has no effect.
    fn ensure_at_least(&mut self, now: Timestamp) {
        if now > self.ts {
            self.last_op_was_read = false;
            self.ts = now
        }
    }
}

/// Metadata about an active connection.
pub(crate) struct ConnMeta {
    /// A watch channel shared with the client to inform the client of
    /// cancellation requests. The coordinator sets the contained value to
    /// `Cancelled::Cancelled` whenever it receives a cancellation request that
    /// targets this connection. It is the client's responsibility to check this
    /// value when appropriate and to reset the value to
    /// `Cancelled::NotCancelled` before starting a new operation.
    pub(crate) cancel_tx: Arc<watch::Sender<Cancelled>>,
    /// Pgwire specifies that every connection have a 32-bit secret associated
    /// with it, that is known to both the client and the server. Cancellation
    /// requests are required to authenticate with the secret of the connection
    /// that they are targeting.
    pub(crate) secret_key: u32,
}

pub(crate) struct TxnReads {
    pub(crate) timedomain_ids: HashSet<GlobalId>,
    pub(crate) _handles: Vec<AntichainToken<Timestamp>>,
}

impl Coordinator {
    fn num_workers(&self) -> usize {
        self.worker_txs.len()
    }

    fn system_timeline(&mut self) -> &mut Timeline {
        self.timelines
            .entry(TimelineId::EpochMilliseconds)
            .or_insert_with(|| Timeline::new(0))
    }

    // Assign a timestamp for a table write.
    pub(crate) fn get_table_write_ts(&mut self) -> Timestamp {
        self.need_advance = true;
        let now = (self.now)();
        let timeline = self.system_timeline();
        timeline.ensure_at_least(now);
        timeline.get_write_ts()
    }

    pub(crate) fn get_timeline_read_ts(&mut self, timeline: TimelineId) -> Timestamp {
        let timeline = self
            .timelines
            .entry(timeline)
            .or_insert_with(|| Timeline::new(0));
        timeline.get_read_ts()
    }

    /// Advances `timeline` to writes at `now` if it has not yet occurred; if reads
    /// at `now` have already occurred, or the timeline timestamp already passed
    /// `now`, this call has no effect.
    fn ensure_at_least_timeline(&mut self, timeline: TimelineId, now: Timestamp) {
        self.timelines
            .entry(timeline)
            .or_insert_with(|| Timeline::new(now))
            .ensure_at_least(now);
    }

    pub(crate) fn now_datetime(&self) -> DateTime<Utc> {
        to_datetime((self.now)())
    }

    /// Generate a new frontiers object that forwards since changes to since_updates.
    ///
    /// # Panics
    ///
    /// This function panics if called twice with the same `id`.
    fn new_frontiers<I>(
        &mut self,
        id: GlobalId,
        initial: I,
        compaction_window_ms: Option<Timestamp>,
    ) -> Frontiers<Timestamp>
    where
        I: IntoIterator<Item = Timestamp>,
    {
        let since_updates = Rc::clone(&self.since_updates);
        let (frontier, handle) = Frontiers::new(
            self.num_workers(),
            initial,
            compaction_window_ms,
            move |frontier| {
                since_updates.borrow_mut().insert(id, frontier);
            },
        );
        let prev = self.since_handles.insert(id, handle);
        // Ensure we don't double-register ids.
        assert!(prev.is_none());
        frontier
    }

    /// Initializes coordinator state based on the contained catalog. Must be
    /// called after creating the coordinator and before calling the
    /// `Coordinator::serve` method.
    async fn bootstrap(
        &mut self,
        builtin_table_updates: Vec<BuiltinTableUpdate>,
    ) -> Result<(), CoordError> {
        let entries: Vec<_> = self.catalog.entries().cloned().collect();

        // Sources and indexes may be depended upon by other catalog items,
        // insert them first.
        for entry in &entries {
            match entry.item() {
                // Currently catalog item rebuild assumes that sinks and
                // indexes are always built individually and does not store information
                // about how it was built. If we start building multiple sinks and/or indexes
                // using a single dataflow, we have to make sure the rebuild process re-runs
                // the same multiple-build dataflow.
                CatalogItem::Source(_) => {
                    // Inform the timestamper about this source.
                    self.update_timestamper(entry.id(), true);
                    let frontiers =
                        self.new_frontiers(entry.id(), Some(0), self.logical_compaction_window_ms);
                    self.sources.insert(entry.id(), frontiers);
                }
                CatalogItem::Index(_) => {
                    if BUILTINS.logs().any(|log| log.index_id == entry.id()) {
                        // Indexes on logging views are special, as they are
                        // already installed in the dataflow plane via
                        // `dataflow::Command::EnableLogging`. Just teach the
                        // coordinator of their existence, without creating a
                        // dataflow for the index.
                        //
                        // TODO(benesch): why is this hardcoded to 1000?
                        // Should it not be the same logical compaction window
                        // that everything else uses?
                        let frontiers = self.new_frontiers(entry.id(), Some(0), Some(1_000));
                        self.indexes.insert(entry.id(), frontiers);
                    } else {
                        let index_id = entry.id();
                        if let Some((name, description)) = self.prepare_index_build(&index_id) {
                            let df = self.dataflow_builder().build_index_dataflow(
                                name,
                                index_id,
                                description,
                            );
                            self.ship_dataflow(df);
                        }
                    }
                }
                _ => (), // Handled in next loop.
            }
        }

        for entry in entries {
            match entry.item() {
                CatalogItem::View(_) => (),
                CatalogItem::Sink(sink) => {
                    let builder = match &sink.connector {
                        SinkConnectorState::Pending(builder) => builder,
                        SinkConnectorState::Ready(_) => {
                            panic!("sink already initialized during catalog boot")
                        }
                    };
                    let connector = sink_connector::build(builder.clone(), entry.id())
                        .await
                        .with_context(|| format!("recreating sink {}", entry.name()))?;
                    self.handle_sink_connector_ready(entry.id(), entry.oid(), connector)?;
                }
                _ => (), // Handled in prior loop.
            }
        }

        self.send_builtin_table_updates(builtin_table_updates);

        // Announce primary and foreign key relationships.
        if self.logging_enabled {
            for log in BUILTINS.logs() {
                let log_id = &log.id.to_string();
                self.send_builtin_table_updates(
                    log.variant
                        .desc()
                        .typ()
                        .keys
                        .iter()
                        .enumerate()
                        .flat_map(move |(index, key)| {
                            key.iter().map(move |k| {
                                let row = Row::pack_slice(&[
                                    Datum::String(log_id),
                                    Datum::Int64(*k as i64),
                                    Datum::Int64(index as i64),
                                ]);
                                BuiltinTableUpdate {
                                    id: MZ_VIEW_KEYS.id,
                                    row,
                                    diff: 1,
                                }
                            })
                        })
                        .collect(),
                );

                self.send_builtin_table_updates(
                    log.variant
                        .foreign_keys()
                        .into_iter()
                        .enumerate()
                        .flat_map(move |(index, (parent, pairs))| {
                            let parent_id = BUILTINS
                                .logs()
                                .find(|src| src.variant == parent)
                                .unwrap()
                                .id
                                .to_string();
                            pairs.into_iter().map(move |(c, p)| {
                                let row = Row::pack_slice(&[
                                    Datum::String(&log_id),
                                    Datum::Int64(c as i64),
                                    Datum::String(&parent_id),
                                    Datum::Int64(p as i64),
                                    Datum::Int64(index as i64),
                                ]);
                                BuiltinTableUpdate {
                                    id: MZ_VIEW_FOREIGN_KEYS.id,
                                    row,
                                    diff: 1,
                                }
                            })
                        })
                        .collect(),
                );
            }
        }

        Ok(())
    }

    /// Serves the coordinator, receiving commands from users over `cmd_rx`
    /// and feedback from dataflow workers over `feedback_rx`.
    ///
    /// You must call `bootstrap` before calling this method.
    async fn serve(
        mut self,
        internal_cmd_rx: mpsc::UnboundedReceiver<Message>,
        cmd_rx: mpsc::UnboundedReceiver<Command>,
        feedback_rx: mpsc::UnboundedReceiver<dataflow::Response>,
        _timestamper_thread_handle: JoinOnDropHandle<()>,
    ) {
        let (drain_trigger, drain_tripwire) = oneshot::channel::<()>();

        let cmd_stream = UnboundedReceiverStream::new(cmd_rx)
            .map(Message::Command)
            .chain(stream::once(future::ready(Message::Shutdown)));

        let feedback_stream = UnboundedReceiverStream::new(feedback_rx).map(Message::Worker);

        let metric_scraper_stream = self
            .metric_scraper
            .tick_stream()
            .take_until(drain_tripwire)
            .boxed();

        let mut messages = ore::future::select_all_biased(vec![
            // Order matters here. We want to drain internal commands
            // (`internal_cmd_rx` and `feedback_stream`) before processing
            // external commands (`cmd_stream`).
            UnboundedReceiverStream::new(internal_cmd_rx).boxed(),
            feedback_stream.boxed(),
            metric_scraper_stream,
            cmd_stream.boxed(),
        ]);

        while let Some(msg) = messages.next().await {
            match msg {
                Message::Command(cmd) => self.message_command(cmd),
                Message::Worker(worker) => self.message_worker(worker),
                Message::StatementReady(ready) => self.message_statement_ready(ready).await,
                Message::SinkConnectorReady(ready) => self.message_sink_connector_ready(ready),
                Message::WriteLockGrant(write_lock_guard) => {
                    // It's possible to have more incoming write lock grants
                    // than pending writes because of cancellations.
                    self.write_lock_wait_group.pop_front().map(|mut ready| {
                        ready.session.grant_write_lock(write_lock_guard);
                        self.sequence_plan(ready.tx, ready.session, ready.plan);
                    });
                    // N.B. if no deferred plans, write lock is released by drop
                    // here.
                }
                Message::SendDiffs(diffs) => self.message_send_diffs(diffs),
                Message::AdvanceSourceTimestamp(advance) => {
                    self.message_advance_source_timestamp(advance)
                }
                Message::ScrapeMetrics => self.message_scrape_metrics(),
                Message::Shutdown => {
                    self.message_shutdown();
                    break;
                }
            }

            if self.need_advance {
                self.advance_tables();
            }
        }

        // Cleanly drain any pending messages from the worker before shutting
        // down.
        drop(drain_trigger);
        drop(self.internal_cmd_tx);
        while messages.next().await.is_some() {}
    }

    // Advance all tables to the current system timeline's write timestamp. This
    // downgrades the capabilities of all tables, which means that all tables can
    // no longer produce new data before this timestamp.
    fn advance_tables(&mut self) {
        self.need_advance = false;
        // We want to immediately make any writes that have happened observable, so we
        // must close at write_ts + 1.
        let next_ts = self.system_timeline().get_write_ts() + 1;
        if next_ts > self.closed_up_to {
            if let Some(persist_multi) = self.catalog.persist_multi_details() {
                // Close out the timestamp for persisted tables.
                //
                // NB: Keep this method call outside the tokio::spawn. We're
                // guaranteed by persist that writes and seals happen in order,
                // but only if we synchronously wait for the (fast) registration
                // of that work to return.
                let seal_res = persist_multi
                    .write_handle
                    .seal(&persist_multi.all_table_ids, next_ts);
                let _ = tokio::spawn(async move {
                    if let Err(err) = seal_res.into_future().await {
                        // TODO: Linearizability relies on this, bubble up the
                        // error instead.
                        //
                        // EDIT: On further consideration, I think it doesn't
                        // affect correctness if this fails, just availability
                        // of the table.
                        log::error!("failed to seal persisted stream to ts {}: {}", next_ts, err);
                    }
                });
            }

            self.broadcast(dataflow::Command::AdvanceAllLocalInputs {
                advance_to: next_ts,
            });
            self.closed_up_to = next_ts;

            // next_ts is the next minimum write time, but the most recent read time we can
            // serve (because it is closed) is next_ts - 1. Advance the timeline and then
            // mark it as a read so that a future read will use next_ts - 1, but a future
            // write will use next_ts.
            let timeline = self.system_timeline();
            timeline.ensure_at_least(next_ts - 1);
            timeline.get_read_ts();
        }
    }

    fn message_worker(&mut self, dataflow::Response { worker_id, message }: dataflow::Response) {
        match message {
            WorkerFeedback::PeekResponse(conn_id, response) => {
                let (channel, workers_responded) = self
                    .pending_peeks
                    .get_mut(&conn_id)
                    .expect("no more PeekResponses after closing peek channel");

                // Each worker may respond only once to each peek.
                assert!(
                    workers_responded.insert(worker_id),
                    "worker {} responded more than once on conn {}",
                    worker_id,
                    conn_id
                );

                channel
                    .send(response)
                    .expect("Peek endpoint terminated prematurely");

                // We've gotten responses from all workers, close peek
                // channel to propagate response to those awaiting.
                if workers_responded.len() == self.num_workers() {
                    self.pending_peeks.remove(&conn_id);
                };
            }
            WorkerFeedback::TailResponse(sink_id, response) => {
                // We use an `if let` here because the peek could have been cancelled already.
                // We can also potentially receive multiple `Complete` responses, followed by
                // a `Dropped` response.
                if let Some(channel) = self.pending_tails.get_mut(&sink_id) {
                    match response {
                        TailResponse::Rows(rows) => {
                            // TODO(benesch): the lack of backpressure here can result in
                            // unbounded memory usage.
                            let result = channel.send(rows);
                            if result.is_err() {
                                // TODO(benesch): we should actually drop the sink if the
                                // receiver has gone away. E.g. form a DROP SINK command?
                            }
                        }
                        TailResponse::Complete => {
                            // TODO: Indicate this explicitly.
                            self.pending_tails.remove(&sink_id);
                        }
                        TailResponse::Dropped => {
                            // TODO: Could perhaps do this earlier, in response to DROP SINK.
                            self.pending_tails.remove(&sink_id);
                        }
                    }
                }
            }
            WorkerFeedback::FrontierUppers(updates) => {
                for (name, changes) in updates {
                    self.update_upper(&name, changes);
                }
                self.maintenance();
            }
            WorkerFeedback::TimestampBindings(TimestampBindingFeedback { bindings, changes }) => {
                self.catalog
                    .insert_timestamp_bindings(
                        bindings
                            .into_iter()
                            .map(|(id, pid, ts, offset)| (id, pid.to_string(), ts, offset.offset)),
                    )
                    .expect("inserting timestamp bindings cannot fail");

                let mut durability_updates = Vec::new();
                for (source_id, mut changes) in changes {
                    if let Some(source_state) = self.sources.get_mut(&source_id) {
                        // Apply the updates the dataflow worker sent over, and check if there
                        // were any changes to the source's upper frontier.
                        let changes: Vec<_> = source_state
                            .durability
                            .update_iter(changes.drain())
                            .collect();

                        if !changes.is_empty() {
                            // The source's durability frontier changed as a result of the updates sent over
                            // by the dataflow workers. Advance the durability frontier known to the dataflow worker
                            // to indicate that these bindings have been persisted.
                            durability_updates
                                .push((source_id, source_state.durability.frontier().to_owned()));
                        }

                        // Let's also check to see if we can compact any of the bindings we've received.
                        let compaction_ts = if <_ as PartialOrder>::less_equal(
                            &source_state.since.borrow().frontier(),
                            &source_state.durability.frontier(),
                        ) {
                            // In this case we have persisted ahead of the compaction frontier and can safely compact
                            // up to it
                            *source_state
                                .since
                                .borrow()
                                .frontier()
                                .first()
                                .expect("known to exist")
                        } else {
                            // Otherwise, the compaction frontier is ahead of what we've persisted so far, but we can
                            // still potentially compact up whatever we have persisted to this point.
                            // Note that we have to subtract from the durability frontier because it functions as the
                            // least upper bound of whats been persisted, and we decline to compact up to the empty
                            // frontier.
                            source_state
                                .durability
                                .frontier()
                                .first()
                                .unwrap_or(&0)
                                .saturating_sub(1)
                        };

                        self.catalog
                            .compact_timestamp_bindings(source_id, compaction_ts)
                            .expect("compacting timestamp bindings cannot fail");
                    }
                }

                // Announce the new frontiers that have been durably persisted.
                if !durability_updates.is_empty() {
                    self.broadcast(dataflow::Command::DurabilityFrontierUpdates(
                        durability_updates,
                    ));
                }
            }
        }
    }

    /// Validate that all upper frontier updates obey the following invariants:
    ///
    /// 1. The `upper` frontier for each source, index and sink does not go backwards with
    /// upper updates
    /// 2. `upper` never contains any times with negative multiplicity.
    /// 3. `upper` never contains any times with multiplicity greater than `n_workers`
    /// 4. No updates increase the sum of all multiplicities in `upper`.
    ///
    /// Note that invariants 2 - 4 require single dimensional time, and a fixed number of
    /// dataflow workers. If we migrate to multidimensional time then 2 no longer holds, and
    /// 3. relaxes to "the longest chain in `upper` has to have <= n_workers elements" and
    /// 4. relaxes to "no comparable updates increase the sum of all multiplicities in `upper`".
    /// If we ever switch to dynamically scaling the number of dataflow workers then 3 and 4 no
    /// longer hold.
    fn validate_update_iter(
        upper: &mut MutableAntichain<Timestamp>,
        mut changes: ChangeBatch<Timestamp>,
        num_workers: usize,
    ) -> Vec<(Timestamp, i64)> {
        let old_frontier = upper.frontier().to_owned();

        // Validate that no changes correspond to a net addition in the sum of all multiplicities.
        // All updates have to relinquish a time, and optionally, acquire another time.
        // TODO: generalize this to multidimensional times.
        let total_changes = changes
            .iter()
            .map(|(_, change)| *change)
            .fold(0, |acc, x| acc + x);
        assert!(total_changes <= 0);

        let frontier_changes = upper.update_iter(changes.clone().drain()).collect();

        // Make sure no times in `upper` have a negative multiplicity
        for (t, _) in changes.into_inner() {
            let count = upper.count_for(&t);
            assert!(count >= 0);
            assert!(count as usize <= num_workers);
        }

        assert!(<_ as PartialOrder>::less_equal(
            &old_frontier.borrow(),
            &upper.frontier(),
        ));

        frontier_changes
    }

    /// Updates the upper frontier of a named view.
    fn update_upper(&mut self, name: &GlobalId, changes: ChangeBatch<Timestamp>) {
        let num_workers = self.num_workers();
        if let Some(index_state) = self.indexes.get_mut(name) {
            let changes =
                Coordinator::validate_update_iter(&mut index_state.upper, changes, num_workers);

            if !changes.is_empty() {
                // Advance the compaction frontier to trail the new frontier.
                // If the compaction latency is `None` compaction messages are
                // not emitted, and the trace should be broadly useable.
                // TODO: If the frontier advances surprisingly quickly, e.g. in
                // the case of a constant collection, this compaction is actively
                // harmful. We should reconsider compaction policy with an eye
                // towards minimizing unexpected screw-ups.
                if let Some(compaction_window_ms) = index_state.compaction_window_ms {
                    // Decline to compact complete collections. This would have the
                    // effect of making the collection unusable. Instead, we would
                    // prefer to compact collections only when we believe it would
                    // reduce the volume of the collection, but we don't have that
                    // information here.
                    if !index_state.upper.frontier().is_empty() {
                        // The since_handle for this GlobalId should have already been registered with
                        // an AntichainToken. Advance it. Changes to the AntichainToken's frontier
                        // will propagate to the Frontiers' since, and changes to that will propate to
                        // self.since_updates.
                        self.since_handles.get_mut(name).unwrap().maybe_advance(
                            index_state.upper.frontier().iter().map(|time| {
                                compaction_window_ms
                                    * (time.saturating_sub(compaction_window_ms)
                                        / compaction_window_ms)
                            }),
                        );
                    }
                }
            }
        } else if let Some(source_state) = self.sources.get_mut(name) {
            let changes =
                Coordinator::validate_update_iter(&mut source_state.upper, changes, num_workers);

            if !changes.is_empty() {
                if let Some(compaction_window_ms) = source_state.compaction_window_ms {
                    if !source_state.upper.frontier().is_empty() {
                        self.since_handles.get_mut(name).unwrap().maybe_advance(
                            source_state.upper.frontier().iter().map(|time| {
                                compaction_window_ms
                                    * (time.saturating_sub(compaction_window_ms)
                                        / compaction_window_ms)
                            }),
                        );
                    }
                }
            }
        } else if let Some(sink_state) = self.sink_writes.get_mut(name) {
            // Only one dataflow worker should give updates for sinks
            let changes = Coordinator::validate_update_iter(&mut sink_state.frontier, changes, 1);

            if !changes.is_empty() {
                sink_state.advance_source_handles();
            }
        }
    }

    /// Forward the subset of since updates that belong to persisted tables'
    /// primary indexes to the persisted tables themselves.
    ///
    /// TODO: In the future the coordinator should perhaps track a table's upper and
    /// since frontiers directly as it currently does for sources.
    fn persisted_table_allow_compaction(&self, since_updates: &[(GlobalId, Antichain<Timestamp>)]) {
        let mut table_since_updates = vec![];
        for (id, frontier) in since_updates.iter() {
            // HACK: Avoid the "failed to compact persisted tables" error log at
            // restart, by not trying to allow compaction on the minimum
            // timestamp.
            if !frontier
                .elements()
                .iter()
                .any(|x| *x > Timestamp::minimum())
            {
                continue;
            }

            // Not all ids will be present in the catalog however, those that are
            // in the catalog must also have their dependencies in the catalog as
            // well.
            let item = self.catalog.try_get_by_id(*id).map(|e| e.item());
            if let Some(CatalogItem::Index(catalog::Index { on, .. })) = item {
                if let CatalogItem::Table(catalog::Table {
                    persist: Some(persist),
                    ..
                }) = self.catalog.get_by_id(on).item()
                {
                    if self.catalog.default_index_for(*on) == Some(*id) {
                        table_since_updates
                            .push((persist.write_handle.stream_id(), frontier.clone()));
                    }
                }
            }
        }

        if !table_since_updates.is_empty() {
            let persist_multi = match self.catalog.persist_multi_details() {
                Some(multi) => multi,
                None => {
                    log::error!("internal error: persist_multi_details invariant violated");
                    return;
                }
            };

            let compaction_res = persist_multi
                .write_handle
                .allow_compaction(&table_since_updates);
            let _ = tokio::spawn(async move {
                if let Err(err) = compaction_res.into_future().await {
                    // TODO: Do something smarter here
                    log::error!("failed to compact persisted tables: {}", err);
                }
            });
        }
    }

    /// Perform maintenance work associated with the coordinator.
    ///
    /// Primarily, this involves sequencing compaction commands, which should be
    /// issued whenever available.
    fn maintenance(&mut self) {
        // Take this opportunity to drain `since_update` commands.
        // Don't try to compact to an empty frontier. There may be a good reason to do this
        // in principle, but not in any current Mz use case.
        // (For background, see: https://github.com/MaterializeInc/materialize/pull/1113#issuecomment-559281990)
        let since_updates: Vec<_> = self
            .since_updates
            .borrow_mut()
            .drain()
            .filter(|(_, frontier)| frontier != &Antichain::new())
            .collect();

        if !since_updates.is_empty() {
            self.persisted_table_allow_compaction(&since_updates);
            self.broadcast(dataflow::Command::AllowCompaction(since_updates));
        }
    }

    pub(crate) fn ship_sources(&mut self, metadata: Vec<(GlobalId, Option<GlobalId>)>) {
        for (source_id, idx_id) in metadata {
            // Do everything to instantiate the source at the coordinator and
            // inform the timestamper and dataflow workers of its existence before
            // shipping any dataflows that depend on its existence.
            self.update_timestamper(source_id, true);
            let frontiers =
                self.new_frontiers(source_id, Some(0), self.logical_compaction_window_ms);
            self.sources.insert(source_id, frontiers);
            if let Some(index_id) = idx_id {
                if let Some((name, description)) = self.prepare_index_build(&index_id) {
                    let df =
                        self.dataflow_builder()
                            .build_index_dataflow(name, index_id, description);
                    self.ship_dataflow(df);
                }
            }
        }
    }

    /// Return the set of ids in a timedomain and verify timeline correctness.
    ///
    /// When a user starts a transaction, we need to prevent compaction of anything
    /// they might read from. We use a heuristic of "anything in the same database
    /// schemas with the same timeline as whatever the first query is".
    pub(crate) fn timedomain_for(
        &self,
        source_ids: &[GlobalId],
        source_timeline: &Option<TimelineId>,
        conn_id: u32,
    ) -> Result<Vec<GlobalId>, CoordError> {
        let mut timedomain_ids = self
            .catalog
            .schema_adjacent_indexed_relations(&source_ids, conn_id);

        // Filter out ids from different timelines. The timeline code only verifies
        // that the SELECT doesn't cross timelines. The schema-adjacent code looks
        // for other ids in the same database schema.
        timedomain_ids.retain(|&id| {
            let id_timeline = self
                .validate_timeline(vec![id])
                .expect("single id should never fail");
            match (&id_timeline, &source_timeline) {
                // If this id doesn't have a timeline, we can keep it.
                (None, _) => true,
                // If there's no source timeline, we have the option to opt into a timeline,
                // so optimistically choose epoch ms. This is useful when the first query in a
                // transaction is on a static view.
                (Some(id_timeline), None) => id_timeline == &TimelineId::EpochMilliseconds,
                // Otherwise check if timelines are the same.
                (Some(id_timeline), Some(source_timeline)) => id_timeline == source_timeline,
            }
        });

        Ok(timedomain_ids)
    }

    /// A policy for determining the timestamp for a peek.
    ///
    /// Returns the timestamp for the peek and the set indexes used. For the
    /// `Immediately` variant of `when`:
    /// - The timestamp will not be less than any index's `since`.
    /// - The timestamp will prefer to use index `upper`s to provide a fresher
    /// result.
    /// - If `timeline` is specified, the timestamp will not be less than that
    /// timeline's linearizability timestamp (which may be advance to the chosen
    /// timestamp).
    pub(crate) fn determine_timestamp(
        &mut self,
        uses_ids: &[GlobalId],
        when: PeekWhen,
        timeline: Option<TimelineId>,
    ) -> Result<(Timestamp, Vec<GlobalId>), CoordError> {
        // Each involved trace has a validity interval `[since, upper)`.
        // The contents of a trace are only guaranteed to be correct when
        // accumulated at a time greater or equal to `since`, and they
        // are only guaranteed to be currently present for times not
        // greater or equal to `upper`.
        //
        // The plan is to first determine a timestamp, based on the requested
        // timestamp policy, and then determine if it can be satisfied using
        // the compacted arrangements we have at hand. It remains unresolved
        // what to do if it cannot be satisfied (perhaps the query should use
        // a larger timestamp and block, perhaps the user should intervene).
        let (index_ids, unmaterialized_source_ids) = self.catalog.nearest_indexes(uses_ids);

        // Determine the valid lower bound of times that can produce correct outputs.
        // This bound is determined by the arrangements contributing to the query,
        // and does not depend on the transitive sources.
        let mut since = self.indexes.least_valid_since(index_ids.iter().cloned());
        since.join_assign(
            &self
                .sources
                .least_valid_since(unmaterialized_source_ids.iter().cloned()),
        );

        // First determine the candidate timestamp, which is either the explicitly requested
        // timestamp, or the latest timestamp known to be immediately available.
        let timestamp = match when {
            // Explicitly requested timestamps should be respected.
            PeekWhen::AtTimestamp(timestamp) => timestamp,

            // These two strategies vary in terms of which traces drive the
            // timestamp determination process: either the trace itself or the
            // original sources on which they depend.
            PeekWhen::Immediately => {
                if !unmaterialized_source_ids.is_empty() {
                    let mut unmaterialized = vec![];
                    let mut disabled_indexes = vec![];
                    for id in unmaterialized_source_ids {
                        // Determine which sources are unmaterialized and which have disabled indexes
                        let name = self.catalog.get_by_id(&id).name().to_string();
                        let indexes = self.catalog.get_indexes_on(id);
                        if indexes.is_empty() {
                            unmaterialized.push(name);
                        } else {
                            let disabled_index_names = indexes
                                .iter()
                                .filter_map(|id| {
                                    if !self.catalog.is_index_enabled(id) {
                                        Some(self.catalog.get_by_id(&id).name().to_string())
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            disabled_indexes.push((name, disabled_index_names));
                        }
                    }
                    return Err(CoordError::AutomaticTimestampFailure {
                        unmaterialized,
                        disabled_indexes,
                    });
                }

                // Start at the timeline timestamp to provide linearizability. If there's no
                // timeline (something with no sources), we can use end-of-time.
                let mut candidate = if let Some(ref timeline) = timeline {
                    self.get_timeline_read_ts(timeline.clone())
                } else {
                    Timestamp::max_value()
                };

                // If there's an upper, advance to it. This is not required for correctness,
                // but improves freshness of results at the expense of possibly forcing other
                // queries to wait (due to linearizability).
                let upper = self.indexes.greatest_open_upper(index_ids.iter().copied());
                // We peek at the largest element not in advance of `upper`, which
                // involves a subtraction. If `upper` contains a zero timestamp there
                // is no "prior" answer, and we do not want to peek at it as it risks
                // hanging awaiting the response to data that may never arrive.
                //
                // The .get(0) here breaks the antichain abstraction by assuming this antichain
                // has 0 or 1 elements in it. It happens to work because we use a timestamp
                // type that meets that assumption, but would break if we used a more general
                // timestamp.
                if let Some(upper) = upper.elements().get(0) {
                    if *upper > 0 {
                        let upper: u64 = upper.saturating_sub(1);
                        if upper > candidate {
                            candidate = upper;
                        }
                    } else {
                        // There are no complete timestamps yet, so candidate will remain at the
                        // timeline timestamp.
                    }
                } else {
                    // A complete trace can be read in its final form with this time.
                    //
                    // This should only happen for literals that have no sources and non-tail file
                    // sources.
                    candidate = Timestamp::max_value()
                }

                // Special case the EpochMilliseconds (Realtime) timeline, ensuring that it
                // can never progress beyond wall clock now. This needs to be done before
                // ensure_at_least_timeline because it needs to respect the since below.
                if let Some(TimelineId::EpochMilliseconds) = timeline {
                    let now = (self.now)();
                    candidate = cmp::min(candidate, now)
                }

                // If the candidate is not beyond the valid `since` frontier,
                // force it to become so as best as we can. If `since` is empty
                // this will be a no-op, as there is no valid time, but that should
                // then be caught below.
                if !since.less_equal(&candidate) {
                    candidate.advance_by(since.borrow());
                }

                // The timeline, since, and upper are all zero. We believe the user would like
                // us to wait until whatever the first valid timestamp for this query is, but
                // we're not sure what that will be, so we need to error.
                if candidate == 0 {
                    let unstarted = index_ids
                        .into_iter()
                        .filter(|id| {
                            self.indexes
                                .upper_of(id)
                                .expect("id not found")
                                .less_equal(&0)
                        })
                        .collect::<Vec<_>>();
                    return Err(CoordError::IncompleteTimestamp(unstarted));
                }

                if let Some(timeline) = timeline {
                    self.ensure_at_least_timeline(timeline, candidate);
                }

                candidate
            }
        };

        // If the timestamp is greater or equal to some element in `since` we are
        // assured that the answer will be correct.
        if since.less_equal(&timestamp) {
            Ok((timestamp, index_ids))
        } else {
            let invalid_indexes = index_ids.iter().filter_map(|id| {
                let since = self.indexes.since_of(id).expect("id not found");
                if since.less_equal(&timestamp) {
                    None
                } else {
                    Some(since)
                }
            });
            let invalid_sources = unmaterialized_source_ids.iter().filter_map(|id| {
                let since = self.sources.since_of(id).expect("id not found");
                if since.less_equal(&timestamp) {
                    None
                } else {
                    Some(since)
                }
            });
            let invalid = invalid_indexes.chain(invalid_sources).collect::<Vec<_>>();
            coord_bail!(
                "Timestamp ({}) is not valid for all inputs: {:?}",
                timestamp,
                invalid
            );
        }
    }

    /// Determine the frontier of updates to start *from* for a sink based on
    /// `source_id`.
    ///
    /// Updates greater or equal to this frontier will be produced.
    ///
    /// If `timeline` is specified, the timestamp will not be less than that
    /// timeline's linearizability timestamp (which may be advance to the chosen
    /// timestamp).
    pub(crate) fn determine_frontier(
        &mut self,
        source_id: GlobalId,
        timeline: Option<TimelineId>,
    ) -> Antichain<Timestamp> {
        // This function differs from determine_timestamp because sinks/tail don't care
        // about indexes existing or timestamps being complete. If data don't exist
        // yet (upper = 0), it is not a problem for the sink to wait for it. If the
        // timestamp we choose isn't as fresh as possible, that's also fine because we
        // produce timestamps describing when the diff occurred, so users can determine
        // if that's fresh enough.

        // If source_id is already indexed, then nearest_indexes will return the
        // same index that default_index_for does, so we can stick with only using
        // nearest_indexes. We don't care about the indexes being incomplete because
        // callers of this function (CREATE SINK and TAIL) are responsible for creating
        // indexes if needed.
        let (index_ids, unmaterialized_source_ids) = self.catalog.nearest_indexes(&[source_id]);
        let mut since = self.indexes.least_valid_since(index_ids.iter().copied());
        since.join_assign(
            &self
                .sources
                .least_valid_since(unmaterialized_source_ids.iter().copied()),
        );

        let mut candidate = if unmaterialized_source_ids.is_empty() && !index_ids.is_empty() {
            // If the sink does not need to create any indexes and requires at least 1
            // index, use the upper. For something like a static view, the indexes are
            // complete but the index count is 0, and we want 0 instead of max for the
            // time, so we should fall through to the else in that case.
            let upper = self.indexes.greatest_open_upper(index_ids);
            if let Some(ts) = upper.elements().get(0) {
                // We don't need to worry about `ts == 0` like determine_timestamp, because
                // it's fine to not have any timestamps completed yet, which will just cause
                // this sink to wait.
                ts.saturating_sub(1)
            } else {
                Timestamp::max_value()
            }
        } else {
            // If the sink does need to create an index, use 0, which will cause the since
            // to be used below.
            Timestamp::min_value()
        };

        // Special case the EpochMilliseconds (Realtime) timeline, ensuring that it
        // can never progress beyond wall clock now. This needs to be done before
        // ensure_at_least_timeline because it needs to respect the since below.
        if let Some(TimelineId::EpochMilliseconds) = timeline {
            let now = (self.now)();
            candidate = cmp::min(candidate, now)
        }

        // Ensure that the timestamp is >= since. This is necessary because when a
        // Frontiers is created, its upper = 0, but the since is > 0 until update_upper
        // has run.
        if !since.less_equal(&candidate) {
            candidate.advance_by(since.borrow());
        }
        if let Some(timeline) = timeline {
            let timestamp = self.get_timeline_read_ts(timeline.clone());
            if candidate < timestamp {
                candidate = timestamp;
            } else {
                self.ensure_at_least_timeline(timeline, candidate);
            }
        }
        Antichain::from_elem(candidate)
    }

    pub(crate) fn catalog_transact(&mut self, ops: Vec<catalog::Op>) -> Result<(), CoordError> {
        let mut sources_to_drop = vec![];
        let mut sinks_to_drop = vec![];
        let mut indexes_to_drop = vec![];
        let mut replication_slots_to_drop: HashMap<String, Vec<String>> = HashMap::new();

        for op in &ops {
            if let catalog::Op::DropItem(id) = op {
                match self.catalog.get_by_id(id).item() {
                    CatalogItem::Table(_) => {
                        sources_to_drop.push(*id);
                    }
                    CatalogItem::Source(source) => {
                        sources_to_drop.push(*id);
                        if let SourceConnector::External {
                            connector:
                                ExternalSourceConnector::Postgres(PostgresSourceConnector {
                                    conn,
                                    slot_name,
                                    ..
                                }),
                            ..
                        } = &source.connector
                        {
                            replication_slots_to_drop
                                .entry(conn.clone())
                                .or_insert_with(Vec::new)
                                .push(slot_name.clone());
                        }
                    }
                    CatalogItem::Sink(catalog::Sink {
                        connector: SinkConnectorState::Ready(_),
                        ..
                    }) => {
                        sinks_to_drop.push(*id);
                    }
                    CatalogItem::Index(_) => {
                        indexes_to_drop.push(*id);
                    }
                    _ => (),
                }
            }
        }

        let builtin_table_updates = self.catalog.transact(ops)?;
        self.send_builtin_table_updates(builtin_table_updates);

        if !sources_to_drop.is_empty() {
            for &id in &sources_to_drop {
                self.update_timestamper(id, false);
                self.catalog.delete_timestamp_bindings(id)?;
                self.sources.remove(&id);
            }
            self.broadcast(dataflow::Command::DropSources(sources_to_drop));
        }
        if !sinks_to_drop.is_empty() {
            for id in sinks_to_drop.iter() {
                self.sink_writes.remove(id);
            }
            self.broadcast(dataflow::Command::DropSinks(sinks_to_drop));
        }
        if !indexes_to_drop.is_empty() {
            self.drop_indexes(indexes_to_drop);
        }

        // We don't want to block the coordinator on an external postgres server, so
        // move the drop slots to a separate task. This does mean that a failed drop
        // slot won't bubble up to the user as an error message. However, even if it
        // did (and how the code previously worked), mz has already dropped it from our
        // catalog, and so we wouldn't be able to retry anyway.
        if !replication_slots_to_drop.is_empty() {
            tokio::spawn(async move {
                for (conn, slot_names) in replication_slots_to_drop {
                    // Try to drop the replication slots, but give up after a while.
                    let _ = Retry::default()
                        .retry(|_state| postgres_util::drop_replication_slots(&conn, &slot_names))
                        .await;
                }
            });
        }

        Ok(())
    }

    pub(crate) fn send_builtin_table_updates_at_offset(&mut self, updates: Vec<TimestampedUpdate>) {
        // NB: This makes sure to send all records for the same id in the same
        // message so we can persist a record and its future retraction
        // atomically. Otherwise, we may end up with permanent orphans if a
        // restart/crash happens at the wrong time.
        let timestamp_base = self.get_table_write_ts();
        let mut updates_by_id = HashMap::<GlobalId, Vec<Update>>::new();
        for tu in updates.into_iter() {
            let timestamp = timestamp_base + tu.timestamp_offset;
            for u in tu.updates {
                updates_by_id.entry(u.id).or_default().push(Update {
                    row: u.row,
                    diff: u.diff,
                    timestamp,
                });
            }
        }
        for (id, updates) in updates_by_id {
            // TODO: It'd be nice to unify this with the similar logic in
            // sequence_end_transaction, but it's not initially clear how to do
            // that.
            let persist = self.catalog.try_get_by_id(id).and_then(|catalog_entry| {
                match catalog_entry.item() {
                    CatalogItem::Table(t) => t.persist.as_ref(),
                    _ => None,
                }
            });
            if let Some(persist) = persist {
                let updates: Vec<((Row, ()), Timestamp, Diff)> = updates
                    .into_iter()
                    .map(|u| ((u.row, ()), u.timestamp, u.diff))
                    .collect();
                // Persistence of system table inserts is best effort, so throw
                // away the response and ignore any errors. We do, however,
                // respect the note below so we don't end up with unexpected
                // write and seal reorderings.
                //
                // NB: Keep this method call outside the tokio::spawn. We're
                // guaranteed by persist that writes and seals happen in order,
                // but only if we synchronously wait for the (fast) registration
                // of that work to return.
                let write_res = persist.write_handle.write(&updates);
                let _ = tokio::spawn(async move { write_res.into_future().await });
            } else {
                self.broadcast(dataflow::Command::Insert { id, updates })
            }
        }
    }

    fn send_builtin_table_updates(&mut self, updates: Vec<BuiltinTableUpdate>) {
        let timestamped = TimestampedUpdate {
            updates,
            timestamp_offset: 0,
        };
        self.send_builtin_table_updates_at_offset(vec![timestamped])
    }

    pub(crate) fn drop_indexes(&mut self, indexes: Vec<GlobalId>) {
        let mut trace_keys = Vec::new();
        for id in indexes {
            if self.indexes.remove(&id).is_some() {
                trace_keys.push(id);
            }
        }
        if !trace_keys.is_empty() {
            self.broadcast(dataflow::Command::DropIndexes(trace_keys))
        }
    }

    /// Finalizes a dataflow and then broadcasts it to all workers.
    /// Utility method for the more general [Self::ship_dataflows]
    pub(crate) fn ship_dataflow(&mut self, dataflow: DataflowDesc) {
        self.ship_dataflows(vec![dataflow])
    }

    /// Finalizes a list of dataflows and then broadcasts it to all workers.
    pub(crate) fn ship_dataflows(&mut self, dataflows: Vec<DataflowDesc>) {
        let mut dataflow_plans = Vec::with_capacity(dataflows.len());
        for dataflow in dataflows.into_iter() {
            dataflow_plans.push(self.finalize_dataflow(dataflow));
        }
        self.broadcast(dataflow::Command::CreateDataflows(dataflow_plans));
    }

    /// Finalizes a dataflow.
    ///
    /// Finalization includes optimization, but also validation of various
    /// invariants such as ensuring that the `as_of` frontier is in advance of
    /// the various `since` frontiers of participating data inputs.
    ///
    /// In particular, there are requirement on the `as_of` field for the dataflow
    /// and the `since` frontiers of created arrangements, as a function of the `since`
    /// frontiers of dataflow inputs (sources and imported arrangements).
    ///
    /// # Panics
    ///
    /// Panics if as_of is < the `since` frontiers.
    ///
    /// Panics if the dataflow descriptions contain an invalid plan.
    pub(crate) fn finalize_dataflow(
        &mut self,
        mut dataflow: DataflowDesc,
    ) -> dataflow_types::DataflowDescription<dataflow::Plan> {
        // This function must succeed because catalog_transact has generally been run
        // before calling this function. We don't have plumbing yet to rollback catalog
        // operations if this function fails, and materialized will be in an unsafe
        // state if we do not correctly clean up the catalog.

        // The identity for `join` is the minimum element.
        let mut since = Antichain::from_elem(Timestamp::minimum());

        // Populate "valid from" information for each source.
        for (source_id, _description) in dataflow.source_imports.iter() {
            // Extract `since` information about each source and apply here.
            if let Some(source_since) = self.sources.since_of(source_id) {
                since.join_assign(&source_since);
            }
        }

        // For each imported arrangement, lower bound `since` by its own frontier.
        for (global_id, (_description, _typ)) in dataflow.index_imports.iter() {
            since.join_assign(
                &self
                    .indexes
                    .since_of(global_id)
                    .expect("global id missing at coordinator"),
            );
        }

        // For each produced arrangement, start tracking the arrangement with
        // a compaction frontier of at least `since`.
        for (global_id, _description, _typ) in dataflow.index_exports.iter() {
            let frontiers = self.new_frontiers(
                *global_id,
                since.elements().to_vec(),
                self.logical_compaction_window_ms,
            );
            self.indexes.insert(*global_id, frontiers);
        }

        // TODO: Produce "valid from" information for each sink.
        // For each sink, ... do nothing because we don't yield `since` for sinks.
        // for (global_id, _description) in dataflow.sink_exports.iter() {
        //     // TODO: assign `since` to a "valid from" element of the sink. E.g.
        //     self.sink_info[global_id].valid_from(&since);
        // }

        // Ensure that the dataflow's `as_of` is at least `since`.
        if let Some(as_of) = &mut dataflow.as_of {
            // It should not be possible to request an invalid time. SINK doesn't support
            // AS OF. TAIL and Peek check that their AS OF is >= since.
            assert!(
                <_ as PartialOrder>::less_equal(&since, as_of),
                "Dataflow {} requested as_of ({:?}) not >= since ({:?})",
                dataflow.debug_name,
                as_of,
                since
            );
        } else {
            // Bind the since frontier to the dataflow description.
            dataflow.set_as_of(since);
        }

        // Optimize the dataflow across views, and any other ways that appeal.
        transform::optimize_dataflow(&mut dataflow, self.catalog.enabled_indexes());
        dataflow::Plan::finalize_dataflow(dataflow)
            .expect("Dataflow planning failed; unrecoverable error")
    }

    pub fn broadcast(&self, cmd: dataflow::Command) {
        for index in 1..self.worker_txs.len() {
            self.worker_txs[index - 1]
                .send(cmd.clone())
                .expect("worker command receiver should not drop first")
        }
        if self.worker_txs.len() > 0 {
            self.worker_txs[self.worker_txs.len() - 1]
                .send(cmd)
                .expect("worker command receiver should not drop first")
        }
        for handle in self.worker_guards.guards() {
            handle.thread().unpark()
        }
    }

    // Notify the timestamper thread that a source has been created or dropped.
    fn update_timestamper(&mut self, source_id: GlobalId, create: bool) {
        if create {
            let bindings = self
                .catalog
                .load_timestamp_bindings(source_id)
                .expect("loading timestamps from coordinator cannot fail");
            if let Some(entry) = self.catalog.try_get_by_id(source_id) {
                if let CatalogItem::Source(s) = entry.item() {
                    self.ts_tx
                        .send(TimestampMessage::Add(source_id, s.connector.clone()))
                        .expect("Failed to send CREATE Instance notice to timestamper");
                    self.broadcast(dataflow::Command::AddSourceTimestamping {
                        id: source_id,
                        connector: s.connector.clone(),
                        bindings,
                    });
                }
            }
        } else {
            self.ts_tx
                .send(TimestampMessage::Drop(source_id))
                .expect("Failed to send DROP Instance notice to timestamper");
            self.broadcast(dataflow::Command::DropSourceTimestamping { id: source_id });
        }
    }

    pub(crate) fn allocate_transient_id(&mut self) -> Result<GlobalId, CoordError> {
        let id = self.transient_id_counter;
        if id == u64::max_value() {
            coord_bail!("id counter overflows i64");
        }
        self.transient_id_counter += 1;
        Ok(GlobalId::Transient(id))
    }

    /// Return an error if the ids are from incompatible timelines. This should
    /// be used to prevent users from doing things that are either meaningless
    /// (joining data from timelines that have similar numbers with different
    /// meanings like two separate debezium topics) or will never complete (joining
    /// byo and realtime data).
    pub(crate) fn validate_timeline(
        &self,
        mut ids: Vec<GlobalId>,
    ) -> Result<Option<TimelineId>, CoordError> {
        let mut timelines: HashMap<GlobalId, TimelineId> = HashMap::new();

        // Recurse through IDs to find all sources and tables, adding new ones to
        // the set until we reach the bottom. Static views will end up with an empty
        // timelines.
        while let Some(id) = ids.pop() {
            // Protect against possible infinite recursion. Not sure if it's possible, but
            // a cheap prevention for the future.
            if timelines.contains_key(&id) {
                continue;
            }
            let entry = self.catalog.get_by_id(&id);
            match entry.item() {
                CatalogItem::Source(source) => {
                    timelines.insert(id, source.connector.timeline());
                }
                CatalogItem::Index(index) => {
                    ids.push(index.on);
                }
                CatalogItem::View(view) => {
                    ids.extend(view.optimized_expr.global_uses());
                }
                CatalogItem::Table(table) => {
                    timelines.insert(id, table.timeline());
                }
                _ => {}
            }
        }

        let timelines: HashSet<TimelineId> = timelines
            .into_iter()
            .map(|(_, timeline)| timeline)
            .collect();

        // If there's more than one timeline, we will not produce meaningful
        // data to a user. Take, for example, some realtime source and a debezium
        // consistency topic source. The realtime source uses something close to now
        // for its timestamps. The debezium source starts at 1 and increments per
        // transaction. We don't want to choose some timestamp that is valid for both
        // of these because the debezium source will never get to the same value as the
        // realtime source's "milliseconds since Unix epoch" value. And even if it did,
        // it's not meaningful to join just because those two numbers happen to be the
        // same now.
        //
        // Another example: assume two separate debezium consistency topics. Both
        // start counting at 1 and thus have similarish numbers that probably overlap
        // a lot. However it's still not meaningful to join those two at a specific
        // transaction counter number because those counters are unrelated to the
        // other.
        if timelines.len() > 1 {
            return Err(CoordError::Unsupported(
                "multiple timelines within one dataflow",
            ));
        }
        Ok(timelines.into_iter().next())
    }
}

/// Serves the coordinator based on the provided configuration.
///
/// For a high-level description of the coordinator, see the [crate
/// documentation](crate).
///
/// Returns a handle to the coordinator and a client to communicate with the
/// coordinator.
pub async fn serve(
    Config {
        workers,
        timely_worker,
        symbiosis_url,
        logging,
        data_directory,
        timestamp_frequency,
        logical_compaction_window,
        experimental_mode,
        disable_user_indexes,
        safe_mode,
        build_info,
        metrics_registry,
        persist,
    }: Config<'_>,
) -> Result<(Handle, Client), CoordError> {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (feedback_tx, feedback_rx) = mpsc::unbounded_channel();
    let (internal_cmd_tx, internal_cmd_rx) = mpsc::unbounded_channel();

    let symbiosis = if let Some(symbiosis_url) = symbiosis_url {
        Some(symbiosis::Postgres::open_and_erase(symbiosis_url).await?)
    } else {
        None
    };

    let path = data_directory.join("catalog");
    let (catalog, builtin_table_updates, persister) = Catalog::open(&catalog::Config {
        path: &path,
        experimental_mode: Some(experimental_mode),
        safe_mode,
        enable_logging: logging.is_some(),
        build_info,
        num_workers: workers,
        timestamp_frequency,
        now: system_time,
        persist,
        skip_migrations: false,
        metrics_registry: &metrics_registry,
        disable_user_indexes,
    })?;
    let cluster_id = catalog.config().cluster_id;
    let session_id = catalog.config().session_id;
    let start_instant = catalog.config().start_instant;

    let (worker_txs, worker_rxs): (Vec<_>, Vec<_>) =
        (0..workers).map(|_| crossbeam_channel::unbounded()).unzip();
    let worker_guards = dataflow::serve(dataflow::Config {
        command_receivers: worker_rxs,
        timely_worker,
        experimental_mode,
        now: system_time,
        metrics_registry: metrics_registry.clone(),
        persist: persister,
        feedback_tx,
    })
    .map_err(|s| CoordError::Unstructured(anyhow!("{}", s)))?;

    let metric_scraper = Scraper::new(logging.as_ref(), metrics_registry.clone())?;

    // Spawn timestamper after any fallible operations so that if bootstrap fails we still
    // tell it to shut down.
    let (ts_tx, ts_rx) = std::sync::mpsc::channel();
    let mut timestamper = Timestamper::new(
        Duration::from_millis(10),
        internal_cmd_tx.clone(),
        ts_rx,
        &metrics_registry,
    );
    let executor = TokioHandle::current();
    let timestamper_thread_handle = thread::Builder::new()
        .name("timestamper".to_string())
        .spawn(move || {
            let _executor_guard = executor.enter();
            timestamper.run();
        })
        .unwrap()
        .join_on_drop();

    // In order for the coordinator to support Rc and Refcell types, it cannot be
    // sent across threads. Spawn it in a thread and have this parent thread wait
    // for bootstrap completion before proceeding.
    let (bootstrap_tx, bootstrap_rx) = std::sync::mpsc::channel();
    let handle = TokioHandle::current();
    let thread = thread::Builder::new()
        .name("coordinator".to_string())
        .spawn(move || {
            let now = catalog.config().now;
            let mut coord = Coordinator {
                worker_guards,
                worker_txs,
                view_optimizer: Optimizer::logical_optimizer(),
                catalog,
                symbiosis,
                indexes: ArrangementFrontiers::default(),
                sources: ArrangementFrontiers::default(),
                logical_compaction_window_ms: logical_compaction_window
                    .map(duration_to_timestamp_millis),
                logging_enabled: logging.is_some(),
                internal_cmd_tx,
                ts_tx: ts_tx.clone(),
                metric_scraper,
                closed_up_to: 1,
                need_advance: true,
                transient_id_counter: 1,
                active_conns: HashMap::new(),
                txn_reads: HashMap::new(),
                since_handles: HashMap::new(),
                since_updates: Rc::new(RefCell::new(HashMap::new())),
                sink_writes: HashMap::new(),
                timelines: HashMap::new(),
                now,
                pending_peeks: HashMap::new(),
                pending_tails: HashMap::new(),
                write_lock: Arc::new(tokio::sync::Mutex::new(())),
                write_lock_wait_group: VecDeque::new(),
            };
            if let Some(config) = &logging {
                coord.broadcast(dataflow::Command::EnableLogging(DataflowLoggingConfig {
                    granularity_ns: config.granularity.as_nanos(),
                    active_logs: BUILTINS
                        .logs()
                        .map(|src| (src.variant.clone(), src.index_id))
                        .collect(),
                    log_logging: config.log_logging,
                }));
            }
            let bootstrap = handle.block_on(coord.bootstrap(builtin_table_updates));
            let ok = bootstrap.is_ok();
            bootstrap_tx.send(bootstrap).unwrap();
            if !ok {
                // Tell the timestamper thread to shut down.
                ts_tx.send(TimestampMessage::Shutdown).unwrap();
                // Explicitly drop the timestamper handle here so we can wait for
                // the thread to return.
                drop(timestamper_thread_handle);
                coord.broadcast(dataflow::Command::Shutdown);
                return;
            }
            handle.block_on(coord.serve(
                internal_cmd_rx,
                cmd_rx,
                feedback_rx,
                timestamper_thread_handle,
            ))
        })
        .unwrap();
    match bootstrap_rx.recv().unwrap() {
        Ok(()) => {
            let handle = Handle {
                cluster_id,
                session_id,
                start_instant,
                _thread: thread.join_on_drop(),
            };
            let client = Client::new(cmd_tx);
            Ok((handle, client))
        }
        Err(e) => Err(e),
    }
}

pub fn serve_debug(
    catalog_path: &Path,
    metrics_registry: MetricsRegistry,
) -> (
    JoinOnDropHandle<()>,
    Client,
    tokio::sync::mpsc::UnboundedSender<dataflow::Response>,
    tokio::sync::mpsc::UnboundedReceiver<dataflow::Response>,
    Arc<Mutex<u64>>,
) {
    lazy_static! {
        static ref DEBUG_TIMESTAMP: Arc<Mutex<EpochMillis>> = Arc::new(Mutex::new(0));
    }
    pub fn get_debug_timestamp() -> EpochMillis {
        *DEBUG_TIMESTAMP.lock().unwrap()
    }

    let (catalog, builtin_table_updates, persister) = catalog::Catalog::open(&catalog::Config {
        path: catalog_path,
        enable_logging: true,
        experimental_mode: None,
        safe_mode: false,
        build_info: &DUMMY_BUILD_INFO,
        num_workers: 0,
        timestamp_frequency: Duration::from_millis(1),
        now: get_debug_timestamp,
        persist: PersistConfig::disabled(),
        skip_migrations: false,
        metrics_registry: &MetricsRegistry::new(),
        disable_user_indexes: false,
    })
    .unwrap();

    // We want to be able to control communication from dataflow to the
    // coordinator, so setup an additional channel pair.
    let (feedback_tx, inner_feedback_rx) = mpsc::unbounded_channel();
    let (inner_feedback_tx, feedback_rx) = mpsc::unbounded_channel();

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (internal_cmd_tx, internal_cmd_rx) = mpsc::unbounded_channel();
    let (worker_tx, worker_rx) = crossbeam_channel::unbounded();
    let worker_guards = dataflow::serve(dataflow::Config {
        command_receivers: vec![worker_rx],
        timely_worker: timely::WorkerConfig::default(),
        experimental_mode: true,
        now: get_debug_timestamp,
        metrics_registry: metrics_registry.clone(),
        persist: persister,
        feedback_tx,
    })
    .unwrap();

    let executor = TokioHandle::current();
    let (ts_tx, ts_rx) = std::sync::mpsc::channel();
    let timestamper_thread_handle = thread::Builder::new()
        .name("timestamper".to_string())
        .spawn(move || {
            let _executor_guard = executor.enter();
            loop {
                match ts_rx.recv().unwrap() {
                    TimestampMessage::Shutdown => break,

                    // Allow local and file sources only. We don't need to do anything for these.
                    TimestampMessage::Add(
                        GlobalId::System(_),
                        SourceConnector::Local {
                            timeline: TimelineId::EpochMilliseconds,
                            persisted_name: None,
                        },
                    )
                    | TimestampMessage::Add(
                        GlobalId::User(_),
                        SourceConnector::External {
                            connector: ExternalSourceConnector::File(_),
                            ..
                        },
                    ) => {}
                    // Panic on anything else (like Kafka sources) until we support them.
                    msg => panic!("unexpected {:?}", msg),
                }
            }
        })
        .unwrap()
        .join_on_drop();

    let (bootstrap_tx, bootstrap_rx) = std::sync::mpsc::channel();
    let handle = TokioHandle::current();
    let thread = thread::spawn(move || {
        let mut coord = Coordinator {
            worker_guards,
            worker_txs: vec![worker_tx],
            view_optimizer: Optimizer::logical_optimizer(),
            catalog,
            symbiosis: None,
            indexes: ArrangementFrontiers::default(),
            sources: ArrangementFrontiers::default(),
            logical_compaction_window_ms: None,
            logging_enabled: false,
            internal_cmd_tx,
            ts_tx,
            metric_scraper: Scraper::new(None, metrics_registry).unwrap(),
            closed_up_to: 1,
            need_advance: true,
            transient_id_counter: 1,
            active_conns: HashMap::new(),
            txn_reads: HashMap::new(),
            since_handles: HashMap::new(),
            since_updates: Rc::new(RefCell::new(HashMap::new())),
            sink_writes: HashMap::new(),
            now: get_debug_timestamp,
            pending_peeks: HashMap::new(),
            pending_tails: HashMap::new(),
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
            write_lock_wait_group: VecDeque::new(),
            timelines: HashMap::new(),
        };
        let bootstrap = handle.block_on(coord.bootstrap(builtin_table_updates));
        bootstrap_tx.send(bootstrap).unwrap();
        handle.block_on(coord.serve(
            internal_cmd_rx,
            cmd_rx,
            feedback_rx,
            timestamper_thread_handle,
        ))
    })
    .join_on_drop();
    bootstrap_rx.recv().unwrap().unwrap();
    let client = Client::new(cmd_tx);
    (
        thread,
        client,
        inner_feedback_tx,
        inner_feedback_rx,
        DEBUG_TIMESTAMP.clone(),
    )
}

/// Converts a Duration to a Timestamp representing the number
/// of milliseconds contained in that Duration
pub(crate) fn duration_to_timestamp_millis(d: Duration) -> Timestamp {
    let millis = d.as_millis();
    if millis > Timestamp::max_value() as u128 {
        Timestamp::max_value()
    } else if millis < Timestamp::min_value() as u128 {
        Timestamp::min_value()
    } else {
        millis as Timestamp
    }
}
