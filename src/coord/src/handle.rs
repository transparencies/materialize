// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryFrom;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use futures::future::{self, FutureExt, TryFutureExt};
use rand::Rng;
use timely::progress::{Antichain, ChangeBatch};
use tokio::sync::mpsc;

use dataflow_types::{
    DataflowDesc, ExternalSourceConnector, IndexDesc, PeekResponse, SinkConnector, SourceConnector,
    TailSinkConnector, Update,
};
use dataflow_types::{SinkAsOf, TimelineId};
use expr::{
    ExprHumanizer, GlobalId, MirRelationExpr, MirScalarExpr, NullaryFunc, OptimizedMirRelationExpr,
    RowSetFinishing,
};
use ore::cast::CastFrom;
use repr::adt::numeric;
use repr::{Datum, Diff, RelationDesc, Row, RowArena, Timestamp};
use sql::ast::display::AstDisplay;
use sql::ast::{
    ConnectorType, CreateIndexStatement, CreateSchemaStatement, CreateSinkStatement,
    CreateSourceStatement, CreateTableStatement, DropObjectsStatement, ExplainStage,
    FetchStatement, Ident, InsertSource, ObjectType, Query, Raw, SetExpr, Statement,
};
use sql::catalog::{CatalogError, SessionCatalog as _};
use sql::names::{DatabaseSpecifier, FullName};
use sql::plan::{
    AlterIndexEnablePlan, AlterIndexResetOptionsPlan, AlterIndexSetOptionsPlan,
    AlterItemRenamePlan, CreateDatabasePlan, CreateIndexPlan, CreateRolePlan, CreateSchemaPlan,
    CreateSinkPlan, CreateSourcePlan, CreateTablePlan, CreateTypePlan, CreateViewPlan,
    CreateViewsPlan, DropDatabasePlan, DropItemsPlan, DropRolesPlan, DropSchemaPlan, ExecutePlan,
    ExplainPlan, FetchPlan, IndexOption, IndexOptionName, InsertPlan, MutationKind, PeekPlan,
    PeekWhen, Plan, ReadThenWritePlan, SendDiffsPlan, SetVariablePlan, ShowVariablePlan, Source,
    TailPlan,
};
use sql::plan::{StatementDesc, View};

use crate::catalog::{self, Catalog, CatalogItem, Table};
use crate::command::{
    Cancelled, Command, ExecuteResponse, Response, StartupMessage, StartupResponse,
};
use crate::coord::arrangement_state::SinkWrites;
use crate::coord::{
    duration_to_timestamp_millis, AdvanceSourceTimestamp, ConnMeta, Coordinator, DeferredPlan,
    Message, SendDiffs, SinkConnectorReady, StatementReady, TxnReads,
};
use crate::error::CoordError;
use crate::session::{
    EndTransactionAction, PreparedStatement, Session, Transaction, TransactionOps,
    TransactionStatus, WriteOp,
};
use crate::sink_connector;
use crate::timestamp::TimestampMessage;
use crate::util::ClientTransmitter;

/// Enforces critical section invariants for functions that perform writes to
/// tables, e.g. `INSERT`, `UPDATE`.
///
/// If the provided session doesn't currently hold the write lock, attempts to
/// grant it. If the coord cannot immediately grant the write lock, defers
/// executing the provided plan until the write lock is available, and exits the
/// function.
///
/// # Parameters
/// - `$coord: &mut Coord`
/// - `$tx: ClientTransmitter<ExecuteResponse>`
/// - `mut $session: Session`
/// - `$plan_to_defer: Plan`
///
/// Note that making this a macro rather than a function lets us avoid taking
/// ownership of e.g. session and lets us unilaterally enforce the return when
/// deferring work.
macro_rules! guard_write_critical_section {
    ($coord:expr, $tx:expr, $session:expr, $plan_to_defer: expr) => {
        if !$session.has_write_lock() {
            if $coord.try_grant_session_write_lock(&mut $session).is_err() {
                $coord.defer_write($tx, $session, $plan_to_defer);
                return;
            }
        }
    };
}

impl Coordinator {
    pub(crate) async fn message_statement_ready(
        &mut self,
        StatementReady {
            mut session,
            tx,
            result,
            params,
        }: StatementReady,
    ) {
        match future::ready(result)
            .and_then(|stmt| self.handle_statement(&mut session, stmt, &params))
            .await
        {
            Ok(plan) => self.sequence_plan(tx, session, plan),
            Err(e) => tx.send(Err(e), session),
        }
    }

    pub(crate) fn message_sink_connector_ready(
        &mut self,
        SinkConnectorReady {
            session,
            tx,
            id,
            oid,
            result,
        }: SinkConnectorReady,
    ) {
        match result {
            Ok(connector) => {
                // NOTE: we must not fail from here on out. We have a
                // connector, which means there is external state (like
                // a Kafka topic) that's been created on our behalf. If
                // we fail now, we'll leak that external state.
                if self.catalog.try_get_by_id(id).is_some() {
                    // TODO(benesch): this `expect` here is possibly scary, but
                    // no better solution presents itself. Possibly sinks should
                    // have an error bit, and an error here would set the error
                    // bit on the sink.
                    self.handle_sink_connector_ready(id, oid, connector)
                        .expect("marking sink ready should never fail");
                } else {
                    // Another session dropped the sink while we were
                    // creating the connector. Report to the client that
                    // we created the sink, because from their
                    // perspective we did, as there is state (e.g. a
                    // Kafka topic) they need to clean up.
                }
                tx.send(Ok(ExecuteResponse::CreatedSink { existed: false }), session);
            }
            Err(e) => {
                // Drop the placeholder sink if still present.
                if self.catalog.try_get_by_id(id).is_some() {
                    self.catalog_transact(vec![catalog::Op::DropItem(id)])
                        .expect("deleting placeholder sink cannot fail");
                } else {
                    // Another session may have dropped the placeholder sink while we were
                    // attempting to create the connector, in which case we don't need to do
                    // anything.
                }
                tx.send(Err(e), session);
            }
        }
    }

    pub(crate) fn message_shutdown(&mut self) {
        self.ts_tx.send(TimestampMessage::Shutdown).unwrap();
        self.broadcast(dataflow::Command::Shutdown);
    }

    pub(crate) fn message_send_diffs(
        &mut self,
        SendDiffs {
            mut session,
            tx,
            id,
            diffs,
            kind,
        }: SendDiffs,
    ) {
        match diffs {
            Ok(diffs) => {
                tx.send(
                    self.sequence_send_diffs(
                        &mut session,
                        SendDiffsPlan {
                            id,
                            updates: diffs,
                            kind,
                        },
                    ),
                    session,
                );
            }
            Err(e) => {
                tx.send(Err(e), session);
            }
        }
    }

    pub(crate) fn message_command(&mut self, cmd: Command) {
        match cmd {
            Command::Startup {
                session,
                cancel_tx,
                tx,
            } => {
                if let Err(e) = self.catalog.create_temporary_schema(session.conn_id()) {
                    let _ = tx.send(Response {
                        result: Err(e.into()),
                        session,
                    });
                    return;
                }

                let catalog = self.catalog.for_session(&session);
                if catalog.resolve_role(session.user()).is_err() {
                    let _ = tx.send(Response {
                        result: Err(CoordError::UnknownLoginRole(session.user().into())),
                        session,
                    });
                    return;
                }

                let mut messages = vec![];
                if catalog
                    .resolve_database(catalog.default_database())
                    .is_err()
                {
                    messages.push(StartupMessage::UnknownSessionDatabase(
                        catalog.default_database().into(),
                    ));
                }

                let secret_key = rand::thread_rng().gen();

                self.active_conns.insert(
                    session.conn_id(),
                    ConnMeta {
                        cancel_tx,
                        secret_key,
                    },
                );

                ClientTransmitter::new(tx).send(
                    Ok(StartupResponse {
                        messages,
                        secret_key,
                    }),
                    session,
                )
            }

            Command::Execute {
                portal_name,
                session,
                tx,
            } => {
                let result = session
                    .get_portal(&portal_name)
                    .ok_or(CoordError::UnknownCursor(portal_name));
                let portal = match result {
                    Ok(portal) => portal,
                    Err(e) => {
                        let _ = tx.send(Response {
                            result: Err(e),
                            session,
                        });
                        return;
                    }
                };
                let stmt = portal.stmt.clone();
                let params = portal.parameters.clone();

                match stmt {
                    Some(stmt) => {
                        // Verify that this statetement type can be executed in the current
                        // transaction state.
                        match session.transaction() {
                            // By this point we should be in a running transaction.
                            &TransactionStatus::Default => unreachable!(),

                            // Started is almost always safe (started means there's a single statement
                            // being executed). Failed transactions have already been checked in pgwire for
                            // a safe statement (COMMIT, ROLLBACK, etc.) and can also proceed.
                            &TransactionStatus::Started(_) | &TransactionStatus::Failed(_) => {
                                if let Statement::Declare(_) = stmt {
                                    // Declare is an exception. Although it's not against any spec to execute
                                    // it, it will always result in nothing happening, since all portals will be
                                    // immediately closed. Users don't know this detail, so this error helps them
                                    // understand what's going wrong. Postgres does this too.
                                    let _ = tx.send(Response {
                                        result: Err(CoordError::OperationRequiresTransaction(
                                            "DECLARE CURSOR".into(),
                                        )),
                                        session,
                                    });
                                    return;
                                }
                            }

                            // Implicit or explicit transactions.
                            //
                            // Implicit transactions happen when a multi-statement query is executed
                            // (a "simple query"). However if a "BEGIN" appears somewhere in there,
                            // then the existing implicit transaction will be upgraded to an explicit
                            // transaction. Thus, we should not separate what implicit and explicit
                            // transactions can do unless there's some additional checking to make sure
                            // something disallowed in explicit transactions did not previously take place
                            // in the implicit portion.
                            &TransactionStatus::InTransactionImplicit(_)
                            | &TransactionStatus::InTransaction(_) => match stmt {
                                // Statements that are safe in a transaction. We still need to verify that we
                                // don't interleave reads and writes since we can't perform those serializably.
                                Statement::Close(_)
                                | Statement::Commit(_)
                                | Statement::Copy(_)
                                | Statement::Deallocate(_)
                                | Statement::Declare(_)
                                | Statement::Discard(_)
                                | Statement::Execute(_)
                                | Statement::Explain(_)
                                | Statement::Fetch(_)
                                | Statement::Prepare(_)
                                | Statement::Rollback(_)
                                | Statement::Select(_)
                                | Statement::SetTransaction(_)
                                | Statement::ShowColumns(_)
                                | Statement::ShowCreateIndex(_)
                                | Statement::ShowCreateSink(_)
                                | Statement::ShowCreateSource(_)
                                | Statement::ShowCreateTable(_)
                                | Statement::ShowCreateView(_)
                                | Statement::ShowDatabases(_)
                                | Statement::ShowIndexes(_)
                                | Statement::ShowObjects(_)
                                | Statement::ShowVariable(_)
                                | Statement::StartTransaction(_)
                                | Statement::Tail(_) => {
                                    // Always safe.
                                }

                                Statement::Insert(ref insert_statment)
                                    if matches!(
                                        insert_statment.source,
                                        InsertSource::Query(Query {
                                            body: SetExpr::Values(..),
                                            ..
                                        }) | InsertSource::DefaultValues
                                    ) =>
                                {
                                    // Inserting from default? values statements
                                    // is always safe.
                                }

                                // Statements below must by run singly (in Started).
                                Statement::AlterIndex(_)
                                | Statement::AlterObjectRename(_)
                                | Statement::CreateDatabase(_)
                                | Statement::CreateIndex(_)
                                | Statement::CreateRole(_)
                                | Statement::CreateSchema(_)
                                | Statement::CreateSink(_)
                                | Statement::CreateSource(_)
                                | Statement::CreateTable(_)
                                | Statement::CreateType(_)
                                | Statement::CreateView(_)
                                | Statement::CreateViews(_)
                                | Statement::Delete(_)
                                | Statement::DropDatabase(_)
                                | Statement::DropObjects(_)
                                | Statement::Insert(_)
                                | Statement::SetVariable(_)
                                | Statement::Update(_) => {
                                    let _ = tx.send(Response {
                                        result: Err(CoordError::OperationProhibitsTransaction(
                                            stmt.to_string(),
                                        )),
                                        session,
                                    });
                                    return;
                                }
                            },
                        }

                        if self.catalog.config().safe_mode {
                            if let Err(e) = check_statement_safety(&stmt) {
                                let _ = tx.send(Response {
                                    result: Err(e),
                                    session,
                                });
                                return;
                            }
                        }

                        let internal_cmd_tx = self.internal_cmd_tx.clone();
                        let catalog = self.catalog.for_session(&session);
                        let purify_fut = sql::pure::purify(&catalog, stmt);
                        tokio::spawn(async move {
                            let result = purify_fut.await.map_err(|e| e.into());
                            internal_cmd_tx
                                .send(Message::StatementReady(StatementReady {
                                    session,
                                    tx: ClientTransmitter::new(tx),
                                    result,
                                    params,
                                }))
                                .expect("sending to internal_cmd_tx cannot fail");
                        });
                    }
                    None => {
                        let _ = tx.send(Response {
                            result: Ok(ExecuteResponse::EmptyQuery),
                            session,
                        });
                    }
                }
            }

            Command::Declare {
                name,
                stmt,
                param_types,
                mut session,
                tx,
            } => {
                let result = self.handle_declare(&mut session, name, stmt, param_types);
                let _ = tx.send(Response { result, session });
            }

            Command::Describe {
                name,
                stmt,
                param_types,
                mut session,
                tx,
            } => {
                let result = self.handle_describe(&mut session, name, stmt, param_types);
                let _ = tx.send(Response { result, session });
            }

            Command::CancelRequest {
                conn_id,
                secret_key,
            } => {
                self.handle_cancel(conn_id, secret_key);
            }

            Command::DumpCatalog { session, tx } => {
                // TODO(benesch): when we have RBAC, dumping the catalog should
                // require superuser permissions.

                let _ = tx.send(Response {
                    result: Ok(self.catalog.dump()),
                    session,
                });
            }

            Command::CopyRows {
                id,
                columns,
                rows,
                mut session,
                tx,
            } => {
                let result = self.sequence_copy_rows(&mut session, id, columns, rows);
                let _ = tx.send(Response { result, session });
            }

            Command::Terminate { mut session } => {
                self.handle_terminate(&mut session);
            }

            Command::StartTransaction {
                implicit,
                session,
                tx,
            } => {
                let now = self.now_datetime();
                let session = match implicit {
                    None => session.start_transaction(now),
                    Some(stmts) => session.start_transaction_implicit(now, stmts),
                };
                let _ = tx.send(Response {
                    result: Ok(()),
                    session,
                });
            }

            Command::Commit {
                action,
                session,
                tx,
            } => {
                let tx = ClientTransmitter::new(tx);
                self.sequence_end_transaction(tx, session, action);
            }

            Command::VerifyPreparedStatement {
                name,
                mut session,
                tx,
            } => {
                let result = self.handle_verify_prepared_statement(&mut session, &name);
                let _ = tx.send(Response { result, session });
            }
        }
    }

    pub(crate) fn message_advance_source_timestamp(
        &mut self,
        AdvanceSourceTimestamp { id, update }: AdvanceSourceTimestamp,
    ) {
        self.broadcast(dataflow::Command::AdvanceSourceTimestamp { id, update });
    }

    pub(crate) fn message_scrape_metrics(&mut self) {
        let scraped_metrics = self.metric_scraper.scrape_once();
        self.send_builtin_table_updates_at_offset(scraped_metrics);
    }

    pub(crate) fn sequence_plan(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        plan: Plan,
    ) {
        match plan {
            Plan::CreateDatabase(plan) => {
                tx.send(self.sequence_create_database(plan), session);
            }
            Plan::CreateSchema(plan) => {
                tx.send(self.sequence_create_schema(plan), session);
            }
            Plan::CreateRole(plan) => {
                tx.send(self.sequence_create_role(plan), session);
            }
            Plan::CreateTable(plan) => {
                tx.send(self.sequence_create_table(&mut session, plan), session);
            }
            Plan::CreateSource(plan) => {
                tx.send(self.sequence_create_source(&mut session, plan), session);
            }
            Plan::CreateSink(plan) => {
                self.sequence_create_sink(session, plan, tx);
            }
            Plan::CreateView(plan) => {
                tx.send(self.sequence_create_view(&mut session, plan), session);
            }
            Plan::CreateViews(plan) => {
                tx.send(self.sequence_create_views(&mut session, plan), session);
            }
            Plan::CreateIndex(plan) => {
                tx.send(self.sequence_create_index(plan), session);
            }
            Plan::CreateType(plan) => {
                tx.send(self.sequence_create_type(plan), session);
            }
            Plan::DropDatabase(plan) => {
                tx.send(self.sequence_drop_database(plan), session);
            }
            Plan::DropSchema(plan) => {
                tx.send(self.sequence_drop_schema(plan), session);
            }
            Plan::DropRoles(plan) => {
                tx.send(self.sequence_drop_roles(plan), session);
            }
            Plan::DropItems(plan) => {
                tx.send(self.sequence_drop_items(plan), session);
            }
            Plan::EmptyQuery => {
                tx.send(Ok(ExecuteResponse::EmptyQuery), session);
            }
            Plan::ShowAllVariables => {
                tx.send(self.sequence_show_all_variables(&session), session);
            }
            Plan::ShowVariable(plan) => {
                tx.send(self.sequence_show_variable(&session, plan), session);
            }
            Plan::SetVariable(plan) => {
                tx.send(self.sequence_set_variable(&mut session, plan), session);
            }
            Plan::StartTransaction => {
                let duplicated =
                    matches!(session.transaction(), TransactionStatus::InTransaction(_));
                let session = session.start_transaction(self.now_datetime());
                tx.send(
                    Ok(ExecuteResponse::StartedTransaction { duplicated }),
                    session,
                )
            }

            Plan::CommitTransaction | Plan::AbortTransaction => {
                let action = match plan {
                    Plan::CommitTransaction => EndTransactionAction::Commit,
                    Plan::AbortTransaction => EndTransactionAction::Rollback,
                    _ => unreachable!(),
                };
                self.sequence_end_transaction(tx, session, action);
            }
            Plan::Peek(plan) => {
                tx.send(self.sequence_peek(&mut session, plan), session);
            }
            Plan::Tail(plan) => {
                tx.send(self.sequence_tail(&mut session, plan), session);
            }
            Plan::SendRows(plan) => {
                tx.send(Ok(send_immediate_rows(plan.rows)), session);
            }

            Plan::CopyFrom(plan) => {
                tx.send(
                    Ok(ExecuteResponse::CopyFrom {
                        id: plan.id,
                        columns: plan.columns,
                        params: plan.params,
                    }),
                    session,
                );
            }
            Plan::Explain(plan) => {
                tx.send(self.sequence_explain(&session, plan), session);
            }
            Plan::SendDiffs(plan) => {
                tx.send(self.sequence_send_diffs(&mut session, plan), session);
            }
            Plan::Insert(plan) => {
                self.sequence_insert(tx, session, plan);
            }
            Plan::ReadThenWrite(plan) => {
                self.sequence_read_then_write(tx, session, plan);
            }
            Plan::AlterNoop(plan) => {
                tx.send(
                    Ok(ExecuteResponse::AlteredObject(plan.object_type)),
                    session,
                );
            }
            Plan::AlterItemRename(plan) => {
                tx.send(self.sequence_alter_item_rename(plan), session);
            }
            Plan::AlterIndexSetOptions(plan) => {
                tx.send(self.sequence_alter_index_set_options(plan), session);
            }
            Plan::AlterIndexResetOptions(plan) => {
                tx.send(self.sequence_alter_index_reset_options(plan), session);
            }
            Plan::AlterIndexEnable(plan) => {
                tx.send(self.sequence_alter_index_enable(plan), session);
            }
            Plan::DiscardTemp => {
                self.drop_temp_items(session.conn_id());
                tx.send(Ok(ExecuteResponse::DiscardedTemp), session);
            }
            Plan::DiscardAll => {
                let ret = if let TransactionStatus::Started(_) = session.transaction() {
                    self.drop_temp_items(session.conn_id());
                    let drop_sinks = session.reset();
                    self.drop_sinks(drop_sinks);
                    Ok(ExecuteResponse::DiscardedAll)
                } else {
                    Err(CoordError::OperationProhibitsTransaction(
                        "DISCARD ALL".into(),
                    ))
                };
                tx.send(ret, session);
            }
            Plan::Declare(plan) => {
                let param_types = vec![];
                let res = self
                    .handle_declare(&mut session, plan.name, plan.stmt, param_types)
                    .map(|()| ExecuteResponse::DeclaredCursor);
                tx.send(res, session);
            }
            Plan::Fetch(FetchPlan {
                name,
                count,
                timeout,
            }) => {
                tx.send(
                    Ok(ExecuteResponse::Fetch {
                        name,
                        count,
                        timeout,
                    }),
                    session,
                );
            }
            Plan::Close(plan) => {
                if session.remove_portal(&plan.name) {
                    tx.send(Ok(ExecuteResponse::ClosedCursor), session);
                } else {
                    tx.send(Err(CoordError::UnknownCursor(plan.name)), session);
                }
            }
            Plan::Prepare(plan) => {
                if session
                    .get_prepared_statement_unverified(&plan.name)
                    .is_some()
                {
                    tx.send(Err(CoordError::PreparedStatementExists(plan.name)), session);
                } else {
                    session.set_prepared_statement(
                        plan.name,
                        PreparedStatement::new(
                            Some(plan.stmt),
                            plan.desc,
                            self.catalog.transient_revision(),
                        ),
                    );
                    tx.send(Ok(ExecuteResponse::Prepare), session);
                }
            }
            Plan::Execute(plan) => {
                match self.sequence_execute(&mut session, plan) {
                    Ok(portal_name) => {
                        let internal_cmd_tx = self.internal_cmd_tx.clone();
                        tokio::spawn(async move {
                            internal_cmd_tx
                                .send(Message::Command(Command::Execute {
                                    portal_name,
                                    session,
                                    tx: tx.take(),
                                }))
                                .expect("sending to internal_cmd_tx cannot fail");
                        });
                    }
                    Err(err) => tx.send(Err(err), session),
                };
            }
            Plan::Deallocate(plan) => match plan.name {
                Some(name) => {
                    if session.remove_prepared_statement(&name) {
                        tx.send(Ok(ExecuteResponse::Deallocate { all: false }), session);
                    } else {
                        tx.send(Err(CoordError::UnknownPreparedStatement(name)), session);
                    }
                }
                None => {
                    session.remove_all_prepared_statements();
                    tx.send(Ok(ExecuteResponse::Deallocate { all: true }), session);
                }
            },
        }
    }

    // Returns the name of the portal to execute.
    fn sequence_execute(
        &mut self,
        session: &mut Session,
        plan: ExecutePlan,
    ) -> Result<String, CoordError> {
        // Verify the stmt is still valid.
        self.handle_verify_prepared_statement(session, &plan.name)?;
        let ps = session.get_prepared_statement_unverified(&plan.name);
        match ps {
            Some(ps) => {
                let sql = ps.sql().cloned();
                let desc = ps.desc().clone();
                session.create_new_portal(sql, desc, plan.params, Vec::new())
            }
            None => Err(CoordError::UnknownPreparedStatement(plan.name)),
        }
    }

    fn sequence_create_database(
        &mut self,
        plan: CreateDatabasePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let db_oid = self.catalog.allocate_oid()?;
        let schema_oid = self.catalog.allocate_oid()?;
        let ops = vec![
            catalog::Op::CreateDatabase {
                name: plan.name.clone(),
                oid: db_oid,
            },
            catalog::Op::CreateSchema {
                database_name: DatabaseSpecifier::Name(plan.name),
                schema_name: "public".into(),
                oid: schema_oid,
            },
        ];
        match self.catalog_transact(ops) {
            Ok(_) => Ok(ExecuteResponse::CreatedDatabase { existed: false }),
            Err(CoordError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::DatabaseAlreadyExists(_),
                ..
            })) if plan.if_not_exists => Ok(ExecuteResponse::CreatedDatabase { existed: true }),
            Err(err) => Err(err),
        }
    }

    fn sequence_create_schema(
        &mut self,
        plan: CreateSchemaPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let oid = self.catalog.allocate_oid()?;
        let op = catalog::Op::CreateSchema {
            database_name: plan.database_name,
            schema_name: plan.schema_name,
            oid,
        };
        match self.catalog_transact(vec![op]) {
            Ok(_) => Ok(ExecuteResponse::CreatedSchema { existed: false }),
            Err(CoordError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::SchemaAlreadyExists(_),
                ..
            })) if plan.if_not_exists => Ok(ExecuteResponse::CreatedSchema { existed: true }),
            Err(err) => Err(err),
        }
    }

    fn sequence_create_role(
        &mut self,
        plan: CreateRolePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let oid = self.catalog.allocate_oid()?;
        let op = catalog::Op::CreateRole {
            name: plan.name,
            oid,
        };
        self.catalog_transact(vec![op])
            .map(|_| ExecuteResponse::CreatedRole)
    }

    fn sequence_create_table(
        &mut self,
        session: &Session,
        plan: CreateTablePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let CreateTablePlan {
            name,
            table,
            if_not_exists,
        } = plan;

        let conn_id = if table.temporary {
            Some(session.conn_id())
        } else {
            None
        };
        let table_id = self.catalog.allocate_id()?;
        let mut index_depends_on = table.depends_on.clone();
        index_depends_on.push(table_id);
        let persist = self
            .catalog
            .persist_details(table_id, &name)
            .map_err(|err| anyhow!("{}", err))?;
        let table = catalog::Table {
            create_sql: table.create_sql,
            desc: table.desc,
            defaults: table.defaults,
            conn_id,
            depends_on: table.depends_on,
            persist,
        };
        let index_id = self.catalog.allocate_id()?;
        let mut index_name = name.clone();
        index_name.item += "_primary_idx";
        index_name = self
            .catalog
            .for_session(session)
            .find_available_name(index_name);
        let index = auto_generate_primary_idx(
            index_name.item.clone(),
            name.clone(),
            table_id,
            &table.desc,
            conn_id,
            index_depends_on,
            self.catalog.index_enabled_by_default(&index_id),
        );
        let table_oid = self.catalog.allocate_oid()?;
        let index_oid = self.catalog.allocate_oid()?;
        match self.catalog_transact(vec![
            catalog::Op::CreateItem {
                id: table_id,
                oid: table_oid,
                name,
                item: CatalogItem::Table(table),
            },
            catalog::Op::CreateItem {
                id: index_id,
                oid: index_oid,
                name: index_name,
                item: CatalogItem::Index(index),
            },
        ]) {
            Ok(_) => {
                if let Some((name, description)) = self.prepare_index_build(&index_id) {
                    let df =
                        self.dataflow_builder()
                            .build_index_dataflow(name, index_id, description);
                    self.ship_dataflow(df);
                }
                Ok(ExecuteResponse::CreatedTable { existed: false })
            }
            Err(CoordError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if if_not_exists => Ok(ExecuteResponse::CreatedTable { existed: true }),
            Err(err) => Err(err),
        }
    }

    fn sequence_create_source(
        &mut self,
        session: &mut Session,
        plan: CreateSourcePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        // TODO(petrosagg): remove this check once postgres sources are properly supported
        if matches!(
            plan,
            CreateSourcePlan {
                source: Source {
                    connector: SourceConnector::External {
                        connector: ExternalSourceConnector::Postgres(_),
                        ..
                    },
                    ..
                },
                materialized: false,
                ..
            }
        ) {
            coord_bail!("Unmaterialized Postgres sources are not supported yet");
        }

        let if_not_exists = plan.if_not_exists;
        let (metadata, ops) = self.generate_create_source_ops(session, vec![plan])?;
        match self.catalog_transact(ops) {
            Ok(()) => {
                self.ship_sources(metadata);
                Ok(ExecuteResponse::CreatedSource { existed: false })
            }
            Err(CoordError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if if_not_exists => Ok(ExecuteResponse::CreatedSource { existed: true }),
            Err(err) => Err(err),
        }
    }

    fn sequence_create_sink(
        &mut self,
        session: Session,
        plan: CreateSinkPlan,
        tx: ClientTransmitter<ExecuteResponse>,
    ) {
        let CreateSinkPlan {
            name,
            sink,
            with_snapshot,
            if_not_exists,
        } = plan;

        // First try to allocate an ID and an OID. If either fails, we're done.
        let id = match self.catalog.allocate_id() {
            Ok(id) => id,
            Err(e) => {
                tx.send(Err(e.into()), session);
                return;
            }
        };
        let oid = match self.catalog.allocate_oid() {
            Ok(id) => id,
            Err(e) => {
                tx.send(Err(e.into()), session);
                return;
            }
        };

        // Then try to create a placeholder catalog item with an unknown
        // connector. If that fails, we're done, though if the client specified
        // `if_not_exists` we'll tell the client we succeeded.
        //
        // This placeholder catalog item reserves the name while we create
        // the sink connector, which could take an arbitrarily long time.
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name,
            item: CatalogItem::Sink(catalog::Sink {
                create_sql: sink.create_sql,
                from: sink.from,
                connector: catalog::SinkConnectorState::Pending(sink.connector_builder.clone()),
                envelope: sink.envelope,
                with_snapshot,
                depends_on: sink.depends_on,
            }),
        };
        match self.catalog_transact(vec![op]) {
            Ok(()) => (),
            Err(CoordError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if if_not_exists => {
                tx.send(Ok(ExecuteResponse::CreatedSink { existed: true }), session);
                return;
            }
            Err(e) => {
                tx.send(Err(e), session);
                return;
            }
        }

        // Now we're ready to create the sink connector. Arrange to notify the
        // main coordinator thread when the future completes.
        let connector_builder = sink.connector_builder;
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        tokio::spawn(async move {
            internal_cmd_tx
                .send(Message::SinkConnectorReady(SinkConnectorReady {
                    session,
                    tx,
                    id,
                    oid,
                    result: sink_connector::build(connector_builder, id).await,
                }))
                .expect("sending to internal_cmd_tx cannot fail");
        });
    }

    fn sequence_create_view(
        &mut self,
        session: &Session,
        plan: CreateViewPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let if_not_exists = plan.if_not_exists;
        let (ops, index_id) = self.generate_view_ops(
            session,
            plan.name,
            plan.view,
            plan.replace,
            plan.materialize,
        )?;

        match self.catalog_transact(ops) {
            Ok(()) => {
                if let Some(index_id) = index_id {
                    if let Some((name, description)) = self.prepare_index_build(&index_id) {
                        let df = self.dataflow_builder().build_index_dataflow(
                            name,
                            index_id,
                            description,
                        );
                        self.ship_dataflow(df);
                    }
                }
                Ok(ExecuteResponse::CreatedView { existed: false })
            }
            Err(CoordError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if if_not_exists => Ok(ExecuteResponse::CreatedView { existed: true }),
            Err(err) => Err(err),
        }
    }

    fn sequence_create_views(
        &mut self,
        session: &mut Session,
        plan: CreateViewsPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let mut ops = vec![];
        let mut index_ids = vec![];

        for (name, view) in plan.views {
            let (mut view_ops, index_id) =
                self.generate_view_ops(session, name, view, None, plan.materialize)?;
            ops.append(&mut view_ops);
            if let Some(index_id) = index_id {
                index_ids.push(index_id);
            }
        }

        match self.catalog_transact(ops) {
            Ok(()) => {
                let mut dfs = vec![];
                for index_id in index_ids {
                    if let Some((name, description)) = self.prepare_index_build(&index_id) {
                        let df = self.dataflow_builder().build_index_dataflow(
                            name,
                            index_id,
                            description,
                        );
                        dfs.push(df);
                    }
                }
                self.ship_dataflows(dfs);
                Ok(ExecuteResponse::CreatedView { existed: false })
            }
            Err(_) if plan.if_not_exists => Ok(ExecuteResponse::CreatedView { existed: true }),
            Err(err) => Err(err),
        }
    }

    fn sequence_create_index(
        &mut self,
        plan: CreateIndexPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let CreateIndexPlan {
            name,
            mut index,
            options,
            if_not_exists,
        } = plan;

        for key in &mut index.keys {
            Self::prep_scalar_expr(key, ExprPrepStyle::Static)?;
        }
        let id = self.catalog.allocate_id()?;
        let index = catalog::Index {
            create_sql: index.create_sql,
            keys: index.keys,
            on: index.on,
            conn_id: None,
            depends_on: index.depends_on,
            enabled: self.catalog.index_enabled_by_default(&id),
        };
        let oid = self.catalog.allocate_oid()?;
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name,
            item: CatalogItem::Index(index),
        };
        match self.catalog_transact(vec![op]) {
            Ok(()) => {
                if let Some((name, description)) = self.prepare_index_build(&id) {
                    let df = self
                        .dataflow_builder()
                        .build_index_dataflow(name, id, description);
                    self.ship_dataflow(df);
                    self.set_index_options(id, options).expect("index enabled");
                }

                Ok(ExecuteResponse::CreatedIndex { existed: false })
            }
            Err(CoordError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if if_not_exists => Ok(ExecuteResponse::CreatedIndex { existed: true }),
            Err(err) => Err(err),
        }
    }

    fn set_index_options(
        &mut self,
        id: GlobalId,
        options: Vec<IndexOption>,
    ) -> Result<(), CoordError> {
        let index = match self.indexes.get_mut(&id) {
            Some(index) => index,
            None => {
                if !self.catalog.is_index_enabled(&id) {
                    return Err(CoordError::InvalidAlterOnDisabledIndex(
                        self.catalog.get_by_id(&id).name().to_string(),
                    ));
                } else {
                    panic!("coord indexes out of sync")
                }
            }
        };

        for o in options {
            match o {
                IndexOption::LogicalCompactionWindow(window) => {
                    let window = window.map(duration_to_timestamp_millis);
                    index.set_compaction_window_ms(window);
                }
            }
        }
        Ok(())
    }

    fn sequence_create_type(
        &mut self,
        plan: CreateTypePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let typ = catalog::Type {
            create_sql: plan.typ.create_sql,
            inner: plan.typ.inner.into(),
            depends_on: plan.typ.depends_on,
        };
        let id = self.catalog.allocate_id()?;
        let oid = self.catalog.allocate_oid()?;
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name: plan.name,
            item: CatalogItem::Type(typ),
        };
        match self.catalog_transact(vec![op]) {
            Ok(()) => Ok(ExecuteResponse::CreatedType),
            Err(err) => Err(err),
        }
    }

    fn sequence_drop_database(
        &mut self,
        plan: DropDatabasePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = self.catalog.drop_database_ops(plan.name);
        self.catalog_transact(ops)?;
        Ok(ExecuteResponse::DroppedDatabase)
    }

    fn sequence_drop_schema(
        &mut self,
        plan: DropSchemaPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = self.catalog.drop_schema_ops(plan.name);
        self.catalog_transact(ops)?;
        Ok(ExecuteResponse::DroppedSchema)
    }

    fn sequence_drop_roles(&mut self, plan: DropRolesPlan) -> Result<ExecuteResponse, CoordError> {
        let ops = plan
            .names
            .into_iter()
            .map(|name| catalog::Op::DropRole { name })
            .collect();
        self.catalog_transact(ops)?;
        Ok(ExecuteResponse::DroppedRole)
    }

    fn sequence_drop_items(&mut self, plan: DropItemsPlan) -> Result<ExecuteResponse, CoordError> {
        let ops = self.catalog.drop_items_ops(&plan.items);
        self.catalog_transact(ops)?;
        Ok(match plan.ty {
            ObjectType::Schema => unreachable!(),
            ObjectType::Source => ExecuteResponse::DroppedSource,
            ObjectType::View => ExecuteResponse::DroppedView,
            ObjectType::Table => ExecuteResponse::DroppedTable,
            ObjectType::Sink => ExecuteResponse::DroppedSink,
            ObjectType::Index => ExecuteResponse::DroppedIndex,
            ObjectType::Type => ExecuteResponse::DroppedType,
            ObjectType::Role => unreachable!("DROP ROLE not supported"),
            ObjectType::Object => unreachable!("generic OBJECT cannot be dropped"),
        })
    }

    fn sequence_show_all_variables(
        &mut self,
        session: &Session,
    ) -> Result<ExecuteResponse, CoordError> {
        Ok(send_immediate_rows(
            session
                .vars()
                .iter()
                .map(|v| {
                    Row::pack_slice(&[
                        Datum::String(v.name()),
                        Datum::String(&v.value()),
                        Datum::String(v.description()),
                    ])
                })
                .collect(),
        ))
    }

    fn sequence_show_variable(
        &self,
        session: &Session,
        plan: ShowVariablePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let variable = session.vars().get(&plan.name)?;
        let row = Row::pack_slice(&[Datum::String(&variable.value())]);
        Ok(send_immediate_rows(vec![row]))
    }

    fn sequence_set_variable(
        &self,
        session: &mut Session,
        plan: SetVariablePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        session.vars_mut().set(&plan.name, &plan.value)?;
        Ok(ExecuteResponse::SetVariable { name: plan.name })
    }

    pub(crate) fn sequence_end_transaction(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        mut action: EndTransactionAction,
    ) {
        if EndTransactionAction::Commit == action {
            let txn = session
                .transaction()
                .inner()
                .expect("must be in a transaction");
            if let Transaction {
                ops: TransactionOps::Writes(_),
                ..
            } = txn
            {
                guard_write_critical_section!(self, tx, session, Plan::CommitTransaction);
            }
        }

        // If the transaction has failed, we can only rollback.
        if let (EndTransactionAction::Commit, TransactionStatus::Failed(_)) =
            (&action, session.transaction())
        {
            action = EndTransactionAction::Rollback;
        }
        let response = ExecuteResponse::TransactionExited {
            tag: action.tag(),
            was_implicit: session.transaction().is_implicit(),
        };
        let rx = self.sequence_end_transaction_inner(&mut session, &action);
        match rx {
            Ok(Some(rx)) => {
                tokio::spawn(async move {
                    // The rx returns a Result<(), CoordError>, so we can map the Ok(()) output to
                    // `response`, which will also pass through whatever error we see to tx.
                    let result = rx.await.map(|_| response);
                    tx.send(result, session);
                });
            }
            Ok(None) => {
                tx.send(Ok(response), session);
            }
            Err(err) => {
                tx.send(Err(err), session);
            }
        }
    }

    fn sequence_end_transaction_inner(
        &mut self,
        session: &mut Session,
        action: &EndTransactionAction,
    ) -> Result<Option<impl Future<Output = Result<(), CoordError>>>, CoordError> {
        let txn = self.clear_transaction(session);

        // Although the compaction frontier may have advanced, we do not need to
        // call `maintenance` here because it will soon be called after the next
        // `update_upper`.

        if let EndTransactionAction::Commit = action {
            if let Some(ops) = txn.into_ops() {
                match ops {
                    TransactionOps::Writes(inserts) => {
                        // Although the transaction has a wall_time in its pcx, we use a new
                        // coordinator timestamp here to provide linearizability. The wall_time does
                        // not have to relate to the write time.
                        let timestamp = self.get_table_write_ts();

                        // Separate out which updates were to tables we are
                        // persisting. In practice, we don't enable/disable this
                        // with table-level granularity so it will be all of
                        // them or none of them, which is checked below.
                        let mut persist_streams = Vec::new();
                        let mut persist_updates = Vec::new();
                        let mut volatile_updates = Vec::new();

                        for WriteOp { id, rows } in inserts {
                            // Re-verify this id exists.
                            let catalog_entry =
                                self.catalog.try_get_by_id(id).ok_or_else(|| {
                                    CoordError::SqlCatalog(CatalogError::UnknownItem(
                                        id.to_string(),
                                    ))
                                })?;
                            // This can be empty if, say, a DELETE's WHERE clause had 0 results.
                            if rows.is_empty() {
                                continue;
                            }
                            match catalog_entry.item() {
                                CatalogItem::Table(Table {
                                    persist: Some(persist),
                                    ..
                                }) => {
                                    let updates: Vec<((Row, ()), Timestamp, Diff)> = rows
                                        .into_iter()
                                        .map(|(row, diff)| ((row, ()), timestamp, diff))
                                        .collect();
                                    persist_streams.push(&persist.write_handle);
                                    persist_updates
                                        .push((persist.write_handle.stream_id(), updates));
                                }
                                _ => {
                                    let updates = rows
                                        .into_iter()
                                        .map(|(row, diff)| Update {
                                            row,
                                            diff,
                                            timestamp,
                                        })
                                        .collect();
                                    volatile_updates.push((id, updates));
                                }
                            }
                        }

                        // Write all updates, both persistent and volatile.
                        // Persistence takes care of introducing anything it
                        // writes to the dataflow, so we only need a
                        // Command::Insert for the volatile updates.
                        if !persist_updates.is_empty() {
                            if !volatile_updates.is_empty() {
                                coord_bail!("transaction had mixed persistent and volatile writes");
                            }
                            let persist_multi =
                                self.catalog.persist_multi_details().ok_or_else(|| {
                                    anyhow!(
                                        "internal error: persist_multi_details invariant violated"
                                    )
                                })?;
                            // NB: Keep this method call outside any
                            // tokio::spawns. We're guaranteed by persist that
                            // writes and seals happen in order, but only if we
                            // synchronously wait for the (fast) registration of
                            // that work to return.
                            let write_res =
                                persist_multi.write_handle.write_atomic(persist_updates);
                            let write_res = write_res.into_future().map(|res| match res {
                                Ok(_) => Ok(()),
                                Err(err) => Err(CoordError::Unstructured(anyhow!("{}", err))),
                            });
                            return Ok(Some(write_res));
                        } else {
                            for (id, updates) in volatile_updates {
                                self.broadcast(dataflow::Command::Insert { id, updates });
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(None)
    }

    /// Sequence a peek, determining a timestamp and the most efficient dataflow interaction.
    ///
    /// Peeks are sequenced by assigning a timestamp for evaluation, and then determining and
    /// deploying the most efficient evaluation plan. The peek could evaluate to a constant,
    /// be a simple read out of an existing arrangement, or required a new dataflow to build
    /// the results to return.
    fn sequence_peek(
        &mut self,
        session: &mut Session,
        plan: PeekPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let PeekPlan {
            source,
            when,
            finishing,
            copy_to,
        } = plan;

        let source_ids = source.global_uses();
        let timeline = self.validate_timeline(source_ids.clone())?;
        let conn_id = session.conn_id();
        let in_transaction = matches!(
            session.transaction(),
            &TransactionStatus::InTransaction(_) | &TransactionStatus::InTransactionImplicit(_)
        );
        let timestamp = match (in_transaction, when) {
            // This is an AS OF query, we don't care about any possible transaction
            // timestamp or linearizability.
            (_, when @ PeekWhen::AtTimestamp(_)) => {
                self.determine_timestamp(&source_ids, when, None)?.0
            }
            // For explicit or implicit transactions that do not use AS OF, get the
            // timestamp of the in-progress transaction or create one.
            (true, PeekWhen::Immediately) => {
                let timestamp = session.get_transaction_timestamp(|| {
                    // Determine a timestamp that will be valid for anything in any schema
                    // referenced by the first query.
                    let mut timedomain_ids =
                        self.timedomain_for(&source_ids, &timeline, conn_id)?;

                    // We want to prevent compaction of the indexes consulted by
                    // determine_timestamp, not the ones listed in the query.
                    let (timestamp, timestamp_ids) = self.determine_timestamp(
                        &timedomain_ids,
                        PeekWhen::Immediately,
                        timeline.clone(),
                    )?;
                    // Add the used indexes to the recorded ids.
                    timedomain_ids.extend(&timestamp_ids);
                    let mut handles = vec![];
                    for id in timestamp_ids {
                        handles.push(self.indexes.get(&id).unwrap().since_handle(vec![timestamp]));
                    }
                    self.txn_reads.insert(
                        conn_id,
                        TxnReads {
                            timedomain_ids: timedomain_ids.into_iter().collect(),
                            _handles: handles,
                        },
                    );

                    Ok(timestamp)
                })?;

                // Verify that the references and indexes for this query are in the current
                // read transaction.
                let mut stmt_ids = HashSet::new();
                stmt_ids.extend(source_ids.iter().collect::<HashSet<_>>());
                // Using nearest_indexes here is a hack until #8318 is fixed. It's used because
                // that's what determine_timestamp uses.
                stmt_ids.extend(
                    self.catalog
                        .nearest_indexes(&source_ids)
                        .0
                        .into_iter()
                        .collect::<HashSet<_>>(),
                );
                let read_txn = self.txn_reads.get(&conn_id).unwrap();
                // Find the first reference or index (if any) that is not in the transaction. A
                // reference could be caused by a user specifying an object in a different
                // schema than the first query. An index could be caused by a CREATE INDEX
                // after the transaction started.
                let outside: Vec<_> = stmt_ids.difference(&read_txn.timedomain_ids).collect();
                if !outside.is_empty() {
                    let mut names: Vec<_> = read_txn
                        .timedomain_ids
                        .iter()
                        // This could filter out a view that has been replaced in another transaction.
                        .filter_map(|id| self.catalog.try_get_by_id(*id))
                        .map(|item| item.name().to_string())
                        .collect();
                    let mut outside: Vec<_> = outside
                        .into_iter()
                        .filter_map(|id| self.catalog.try_get_by_id(*id))
                        .map(|item| item.name().to_string())
                        .collect();
                    // Sort so error messages are deterministic.
                    names.sort();
                    outside.sort();
                    return Err(CoordError::RelationOutsideTimeDomain {
                        relations: outside,
                        names,
                    });
                }

                timestamp
            }
            // This is a single-statement transaction (TransactionStatus::Started),
            // we don't need to worry about preventing compaction or choosing a valid
            // timestamp for future queries.
            (false, when @ PeekWhen::Immediately) => {
                self.determine_timestamp(&source_ids, when, timeline)?.0
            }
        };

        let source = self.prep_relation_expr(
            source,
            ExprPrepStyle::OneShot {
                logical_time: timestamp,
            },
        )?;

        // We create a dataflow and optimize it, to determine if we can avoid building it.
        // This can happen if the result optimizes to a constant, or to a `Get` expression
        // around a maintained arrangement.
        let typ = source.typ();
        let key: Vec<MirScalarExpr> = typ
            .default_key()
            .iter()
            .map(|k| MirScalarExpr::Column(*k))
            .collect();
        // Two transient allocations. We could reclaim these if we don't use them, potentially.
        // TODO: reclaim transient identifiers in fast path cases.
        let view_id = self.allocate_transient_id()?;
        let index_id = self.allocate_transient_id()?;
        // The assembled dataflow contains a view and an index of that view.
        let mut dataflow = DataflowDesc::new(format!("temp-view-{}", view_id));
        dataflow.set_as_of(Antichain::from_elem(timestamp));
        self.dataflow_builder()
            .import_view_into_dataflow(&view_id, &source, &mut dataflow);
        dataflow.export_index(
            index_id,
            IndexDesc {
                on_id: view_id,
                keys: key,
            },
            typ,
        );
        // Finalization optimizes the dataflow as much as possible.
        let dataflow_plan = self.finalize_dataflow(dataflow);

        // At this point, `dataflow_plan` contains our best optimized dataflow.
        // We will check the plan to see if there is a fast path to escape full dataflow construction.
        let fast_path = fast_path_peek::create_plan(dataflow_plan, view_id, index_id)?;

        // Implement the peek, and capture the response.
        let resp = self.implement_fast_path_peek(
            fast_path,
            timestamp,
            finishing,
            conn_id,
            source.arity(),
        )?;

        match copy_to {
            None => Ok(resp),
            Some(format) => Ok(ExecuteResponse::CopyTo {
                format,
                resp: Box::new(resp),
            }),
        }
    }

    fn sequence_tail(
        &mut self,
        session: &mut Session,
        plan: TailPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let TailPlan {
            id: source_id,
            with_snapshot,
            ts,
            copy_to,
            emit_progress,
            object_columns,
            desc,
        } = plan;
        // TAIL AS OF, similar to peeks, doesn't need to worry about transaction
        // timestamp semantics.
        if ts.is_none() {
            // If this isn't a TAIL AS OF, the TAIL can be in a transaction if it's the
            // only operation.
            session.add_transaction_ops(TransactionOps::Tail)?;
        }

        // Determine the frontier of updates to tail *from*.
        // Updates greater or equal to this frontier will be produced.
        let frontier = if let Some(ts) = ts {
            // If a timestamp was explicitly requested, use that.
            Antichain::from_elem(
                self.determine_timestamp(&[source_id], PeekWhen::AtTimestamp(ts), None)?
                    .0,
            )
        } else {
            let timeline = self.validate_timeline(vec![source_id])?;
            self.determine_frontier(source_id, timeline)
        };
        let sink_name = format!(
            "tail-source-{}",
            self.catalog
                .for_session(session)
                .humanize_id(source_id)
                .expect("Source id is known to exist in catalog")
        );
        let sink_id = self.catalog.allocate_id()?;
        session.add_drop_sink(sink_id);
        let (tx, rx) = mpsc::unbounded_channel();
        self.pending_tails.insert(sink_id, tx);
        let sink_description = dataflow_types::SinkDesc {
            from: source_id,
            from_desc: self.catalog.get_by_id(&source_id).desc().unwrap().clone(),
            connector: SinkConnector::Tail(TailSinkConnector {
                emit_progress,
                object_columns,
                value_desc: desc,
            }),
            envelope: None,
            as_of: SinkAsOf {
                frontier,
                strict: !with_snapshot,
            },
        };
        let df = self
            .dataflow_builder()
            .build_sink_dataflow(sink_name, sink_id, sink_description);
        self.ship_dataflow(df);

        let resp = ExecuteResponse::Tailing { rx };

        match copy_to {
            None => Ok(resp),
            Some(format) => Ok(ExecuteResponse::CopyTo {
                format,
                resp: Box::new(resp),
            }),
        }
    }

    fn sequence_explain(
        &mut self,
        session: &Session,
        plan: ExplainPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let ExplainPlan {
            raw_plan,
            decorrelated_plan,
            row_set_finishing,
            stage,
            options,
        } = plan;

        let explanation_string = match stage {
            ExplainStage::RawPlan => {
                let catalog = self.catalog.for_session(session);
                let mut explanation = sql::plan::Explanation::new(&raw_plan, &catalog);
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                if options.typed {
                    explanation.explain_types(&BTreeMap::new());
                }
                explanation.to_string()
            }
            ExplainStage::DecorrelatedPlan => {
                let catalog = self.catalog.for_session(session);
                let mut explanation =
                    dataflow_types::Explanation::new(&decorrelated_plan, &catalog);
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                if options.typed {
                    explanation.explain_types();
                }
                explanation.to_string()
            }
            ExplainStage::OptimizedPlan => {
                self.validate_timeline(decorrelated_plan.global_uses())?;
                let optimized_plan =
                    self.prep_relation_expr(decorrelated_plan, ExprPrepStyle::Explain)?;
                let mut dataflow = DataflowDesc::new(format!("explanation"));
                self.dataflow_builder().import_view_into_dataflow(
                    // TODO: If explaining a view, pipe the actual id of the view.
                    &GlobalId::Explain,
                    &optimized_plan,
                    &mut dataflow,
                );
                transform::optimize_dataflow(&mut dataflow, self.catalog.enabled_indexes());
                let catalog = self.catalog.for_session(session);
                let mut explanation =
                    dataflow_types::Explanation::new_from_dataflow(&dataflow, &catalog);
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                if options.typed {
                    explanation.explain_types();
                }
                explanation.to_string()
            }
        };
        let rows = vec![Row::pack_slice(&[Datum::from(&*explanation_string)])];
        Ok(send_immediate_rows(rows))
    }

    pub(crate) fn sequence_send_diffs(
        &mut self,
        session: &mut Session,
        plan: SendDiffsPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        // Take a detour through ChangeBatch so we can consolidate updates. Useful
        // especially for UPDATE where a row shouldn't change.
        let mut rows = ChangeBatch::with_capacity(plan.updates.len());
        rows.extend(
            plan.updates
                .into_iter()
                .map(|(v, sz)| (v, i64::cast_from(sz))),
        );

        // The number of affected rows is not the number of rows we see, but the
        // sum of the abs of their diffs, i.e. we see `INSERT INTO t VALUES (1),
        // (1)` as a row with a value of 1 and a diff of +2.
        let mut affected_rows = 0isize;
        let rows = rows
            .into_inner()
            .into_iter()
            .map(|(v, sz)| {
                let diff = isize::cast_from(sz);
                affected_rows += diff.abs();
                (v, diff)
            })
            .collect();

        let affected_rows = usize::try_from(affected_rows).expect("positive isize must fit");

        session.add_transaction_ops(TransactionOps::Writes(vec![WriteOp { id: plan.id, rows }]))?;
        Ok(match plan.kind {
            MutationKind::Delete => ExecuteResponse::Deleted(affected_rows),
            MutationKind::Insert => {
                if self.catalog.config().disable_user_indexes {
                    self.catalog.ensure_default_index_enabled(plan.id)?;
                }

                ExecuteResponse::Inserted(affected_rows)
            }
            MutationKind::Update => ExecuteResponse::Updated(affected_rows / 2),
        })
    }

    fn sequence_insert(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        plan: InsertPlan,
    ) {
        let optimized_mir = match self.prep_relation_expr(plan.values, ExprPrepStyle::Write) {
            Ok(m) => m,
            Err(e) => {
                tx.send(Err(e), session);
                return;
            }
        };

        match optimized_mir.into_inner() {
            constants @ MirRelationExpr::Constant { .. } => tx.send(
                self.sequence_insert_constant(&mut session, plan.id, constants),
                session,
            ),
            // All non-constant values must be planned as read-then-writes.
            selection => {
                let desc_arity = match self.catalog.try_get_by_id(plan.id) {
                    Some(table) => table.desc().expect("desc called on table").arity(),
                    None => {
                        tx.send(
                            Err(CoordError::SqlCatalog(CatalogError::UnknownItem(
                                plan.id.to_string(),
                            ))),
                            session,
                        );
                        return;
                    }
                };

                let finishing = RowSetFinishing {
                    order_by: vec![],
                    limit: None,
                    offset: 0,
                    project: (0..desc_arity).collect(),
                };

                let read_then_write_plan = ReadThenWritePlan {
                    id: plan.id,
                    selection,
                    finishing,
                    assignments: HashMap::new(),
                    kind: MutationKind::Insert,
                };

                self.sequence_read_then_write(tx, session, read_then_write_plan);
            }
        }
    }

    fn sequence_insert_constant(
        &mut self,
        session: &mut Session,
        id: GlobalId,
        constants: MirRelationExpr,
    ) -> Result<ExecuteResponse, CoordError> {
        // Insert can be queued, so we need to re-verify the id exists.
        let desc = match self.catalog.try_get_by_id(id) {
            Some(table) => table.desc()?,
            None => {
                return Err(CoordError::SqlCatalog(CatalogError::UnknownItem(
                    id.to_string(),
                )))
            }
        };

        match constants {
            MirRelationExpr::Constant { rows, typ: _ } => {
                let rows = rows?;
                for (row, _) in &rows {
                    for (i, datum) in row.unpack().iter().enumerate() {
                        desc.constraints_met(i, datum)?;
                    }
                }
                let diffs_plan = SendDiffsPlan {
                    id,
                    updates: rows,
                    kind: MutationKind::Insert,
                };
                self.sequence_send_diffs(session, diffs_plan)
            }
            o => panic!(
                "tried using sequence_insert_constant on non-constant MirRelationExpr {:?}",
                o
            ),
        }
    }

    pub(crate) fn sequence_copy_rows(
        &mut self,
        session: &mut Session,
        id: GlobalId,
        columns: Vec<usize>,
        rows: Vec<Row>,
    ) -> Result<ExecuteResponse, CoordError> {
        let catalog = self.catalog.for_session(session);
        let values = sql::plan::plan_copy_from(&session.pcx(), &catalog, id, columns, rows)?;

        let constants = self
            .prep_relation_expr(values.lower(), ExprPrepStyle::Write)?
            .into_inner();

        // Copied rows must always be constants.
        self.sequence_insert_constant(session, id, constants)
    }

    // ReadThenWrite is a plan whose writes depend on the results of a
    // read. This works by doing a Peek then queuing a SendDiffs. No writes
    // or read-then-writes can occur between the Peek and SendDiff otherwise a
    // serializability violation could occur.
    fn sequence_read_then_write(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        plan: ReadThenWritePlan,
    ) {
        guard_write_critical_section!(self, tx, session, Plan::ReadThenWrite(plan));

        let ReadThenWritePlan {
            id,
            kind,
            selection,
            assignments,
            finishing,
        } = plan;

        // Read then writes can be queued, so re-verify the id exists.
        let desc = match self.catalog.try_get_by_id(id) {
            Some(table) => table.desc().expect("desc called on table").clone(),
            None => {
                tx.send(
                    Err(CoordError::SqlCatalog(CatalogError::UnknownItem(
                        id.to_string(),
                    ))),
                    session,
                );
                return;
            }
        };

        // Ensure selection targets are valid, i.e. user-defined tables, or
        // objects local to the dataflow.
        for id in selection.global_uses() {
            let valid = match self.catalog.try_get_by_id(id) {
                // TODO: Widen this check when supporting temporary tables.
                Some(entry) if id.is_user() => entry.is_table(),
                _ => false,
            };
            if !valid {
                tx.send(Err(CoordError::InvalidTableMutationSelection), session);
                return;
            }
        }

        // TODO(mjibson): Is there a more principled way to decide the timeline here
        // than hard coding this?
        let ts = self.get_timeline_read_ts(TimelineId::EpochMilliseconds);
        let peek_response = match self.sequence_peek(
            &mut session,
            PeekPlan {
                source: selection,
                when: PeekWhen::AtTimestamp(ts),
                finishing,
                copy_to: None,
            },
        ) {
            Ok(resp) => resp,
            Err(e) => {
                tx.send(Err(e.into()), session);
                return;
            }
        };

        let internal_cmd_tx = self.internal_cmd_tx.clone();
        tokio::spawn(async move {
            let arena = RowArena::new();
            let diffs = match peek_response {
                ExecuteResponse::SendingRows(batch) => match batch.await {
                    PeekResponse::Rows(rows) => {
                        |rows: Vec<Row>| -> Result<Vec<(Row, Diff)>, CoordError> {
                            // Use 2x row len incase there's some assignments.
                            let mut diffs = Vec::with_capacity(rows.len() * 2);
                            for row in rows {
                                if !assignments.is_empty() {
                                    assert!(
                                        matches!(kind, MutationKind::Update),
                                        "only updates support assignments"
                                    );
                                    let mut datums = row.unpack();
                                    let mut updates = vec![];
                                    for (idx, expr) in &assignments {
                                        let updated = match expr.eval(&datums, &arena) {
                                            Ok(updated) => updated,
                                            Err(e) => {
                                                return Err(CoordError::Unstructured(anyhow!(e)))
                                            }
                                        };
                                        desc.constraints_met(*idx, &updated)?;
                                        updates.push((*idx, updated));
                                    }
                                    for (idx, new_value) in updates {
                                        datums[idx] = new_value;
                                    }
                                    let updated = Row::pack_slice(&datums);
                                    diffs.push((updated, 1));
                                }
                                match kind {
                                    // Updates and deletes always remove the
                                    // current row. Updates will also add an
                                    // updated value.
                                    MutationKind::Update | MutationKind::Delete => {
                                        diffs.push((row, -1))
                                    }
                                    MutationKind::Insert => diffs.push((row, 1)),
                                }
                            }
                            Ok(diffs)
                        }(rows)
                    }
                    PeekResponse::Canceled => {
                        Err(CoordError::Unstructured(anyhow!("execution canceled")))
                    }
                    PeekResponse::Error(e) => Err(CoordError::Unstructured(anyhow!(e))),
                },
                _ => Err(CoordError::Unstructured(anyhow!("expected SendingRows"))),
            };
            internal_cmd_tx
                .send(Message::SendDiffs(SendDiffs {
                    session,
                    tx,
                    id,
                    diffs,
                    kind,
                }))
                .expect("sending to internal_cmd_tx cannot fail");
        });
    }

    fn sequence_alter_item_rename(
        &mut self,
        plan: AlterItemRenamePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let op = catalog::Op::RenameItem {
            id: plan.id,
            to_name: plan.to_name,
        };
        match self.catalog_transact(vec![op]) {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(plan.object_type)),
            Err(err) => Err(err),
        }
    }

    fn sequence_alter_index_set_options(
        &mut self,
        plan: AlterIndexSetOptionsPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        self.set_index_options(plan.id, plan.options)?;
        Ok(ExecuteResponse::AlteredObject(ObjectType::Index))
    }

    fn sequence_alter_index_reset_options(
        &mut self,
        plan: AlterIndexResetOptionsPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let options = plan
            .options
            .into_iter()
            .map(|o| match o {
                IndexOptionName::LogicalCompactionWindow => IndexOption::LogicalCompactionWindow(
                    self.logical_compaction_window_ms.map(Duration::from_millis),
                ),
            })
            .collect();
        self.set_index_options(plan.id, options)?;
        Ok(ExecuteResponse::AlteredObject(ObjectType::Index))
    }

    fn sequence_alter_index_enable(
        &mut self,
        plan: AlterIndexEnablePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = self.catalog.enable_index_ops(plan.id)?;

        // If ops is not empty, index was disabled.
        if !ops.is_empty() {
            self.catalog_transact(ops)?;
            let (name, description) = self.prepare_index_build(&plan.id).expect("index enabled");
            let df = self
                .dataflow_builder()
                .build_index_dataflow(name, plan.id, description);
            self.ship_dataflow(df);
        }

        Ok(ExecuteResponse::AlteredObject(ObjectType::Index))
    }

    pub(crate) fn handle_declare(
        &self,
        session: &mut Session,
        name: String,
        stmt: Statement<Raw>,
        param_types: Vec<Option<pgrepr::Type>>,
    ) -> Result<(), CoordError> {
        // handle_describe cares about symbiosis mode here. Declared cursors are
        // perhaps rare enough we can ignore that worry and just error instead.
        let desc = describe(&self.catalog, stmt.clone(), &param_types, session)?;
        let params = vec![];
        let result_formats = vec![pgrepr::Format::Text; desc.arity()];
        session.set_portal(name, desc, Some(stmt), params, result_formats)?;
        Ok(())
    }

    pub(crate) fn handle_describe(
        &self,
        session: &mut Session,
        name: String,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<pgrepr::Type>>,
    ) -> Result<(), CoordError> {
        let desc = self.describe(session, stmt.clone(), param_types)?;
        session.set_prepared_statement(
            name,
            PreparedStatement::new(stmt, desc, self.catalog.transient_revision()),
        );
        Ok(())
    }

    fn describe(
        &self,
        session: &Session,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<pgrepr::Type>>,
    ) -> Result<StatementDesc, CoordError> {
        if let Some(stmt) = stmt {
            // Pre-compute this to avoid cloning stmt.
            let postgres_can_handle = match self.symbiosis {
                Some(ref postgres) => postgres.can_handle(&stmt),
                None => false,
            };
            match describe(&self.catalog, stmt, &param_types, session) {
                Ok(desc) => Ok(desc),
                // Describing the query failed. If we're running in symbiosis with
                // Postgres, see if Postgres can handle it. Note that Postgres
                // only handles commands that do not return rows, so the
                // `StatementDesc` is constructed accordingly.
                Err(_) if postgres_can_handle => Ok(StatementDesc::new(None)),
                Err(err) => Err(err),
            }
        } else {
            Ok(StatementDesc::new(None))
        }
    }

    pub async fn handle_statement(
        &mut self,
        session: &mut Session,
        stmt: sql::ast::Statement<Raw>,
        params: &sql::plan::Params,
    ) -> Result<sql::plan::Plan, CoordError> {
        let pcx = session.pcx();

        // When symbiosis mode is enabled, use symbiosis planning for:
        //  - CREATE TABLE
        //  - CREATE SCHEMA
        //  - DROP TABLE
        //  - INSERT
        //  - UPDATE
        //  - DELETE
        // When these statements are routed through symbiosis, table information
        // is created and maintained locally, which is required for other statements
        // to be executed correctly.
        if let Statement::CreateTable(CreateTableStatement { .. })
        | Statement::DropObjects(DropObjectsStatement {
            object_type: ObjectType::Table,
            ..
        })
        | Statement::CreateSchema(CreateSchemaStatement { .. })
        | Statement::Update { .. }
        | Statement::Delete { .. }
        | Statement::Insert { .. } = &stmt
        {
            if let Some(ref mut postgres) = self.symbiosis {
                let plan = postgres
                    .execute(&pcx, &self.catalog.for_session(session), &stmt)
                    .await?;
                return Ok(plan);
            }
        }

        match sql::plan::plan(
            Some(&pcx),
            &self.catalog.for_session(session),
            stmt.clone(),
            params,
        ) {
            Ok(plan) => Ok(plan),
            Err(err) => match self.symbiosis {
                Some(ref mut postgres) if postgres.can_handle(&stmt) => {
                    let plan = postgres
                        .execute(&pcx, &self.catalog.for_session(session), &stmt)
                        .await?;
                    Ok(plan)
                }
                _ => Err(err.into()),
            },
        }
    }

    /// Verify a prepared statement is still valid.
    pub(crate) fn handle_verify_prepared_statement(
        &self,
        session: &mut Session,
        name: &str,
    ) -> Result<(), CoordError> {
        let ps = match session.get_prepared_statement_unverified(&name) {
            Some(ps) => ps,
            None => return Err(CoordError::UnknownPreparedStatement(name.to_string())),
        };
        if ps.catalog_revision != self.catalog.transient_revision() {
            let desc = self.describe(
                session,
                ps.sql().cloned(),
                ps.desc()
                    .param_types
                    .iter()
                    .map(|ty| Some(ty.clone()))
                    .collect(),
            )?;
            if &desc != ps.desc() {
                Err(CoordError::ChangedPlan)
            } else {
                // If the descs are the same, we can bump our version to declare that ps is
                // correct as of now.
                let ps = session
                    .get_prepared_statement_mut_unverified(name)
                    .expect("known to exist");
                ps.catalog_revision = self.catalog.transient_revision();
                Ok(())
            }
        } else {
            Ok(())
        }
    }

    /// Instruct the dataflow layer to cancel any ongoing, interactive work for
    /// the named `conn_id`.
    pub(crate) fn handle_cancel(&mut self, conn_id: u32, secret_key: u32) {
        if let Some(conn_meta) = self.active_conns.get(&conn_id) {
            // If the secret key specified by the client doesn't match the
            // actual secret key for the target connection, we treat this as a
            // rogue cancellation request and ignore it.
            if conn_meta.secret_key != secret_key {
                return;
            }

            // Allow dataflow to cancel any pending peeks.
            self.broadcast(dataflow::Command::CancelPeek { conn_id });

            // Cancel deferred writes. There is at most one pending write per session.
            if let Some(idx) = self
                .write_lock_wait_group
                .iter()
                .position(|ready| ready.session.conn_id() == conn_id)
            {
                let ready = self.write_lock_wait_group.remove(idx).unwrap();
                ready.tx.send(Ok(ExecuteResponse::Cancelled), ready.session);
            }

            // Inform the target session (if it asks) about the cancellation.
            let _ = conn_meta.cancel_tx.send(Cancelled::Cancelled);
        }
    }

    /// Handle termination of a client session.
    ///
    /// This cleans up any state in the coordinator associated with the session.
    pub(crate) fn handle_terminate(&mut self, session: &mut Session) {
        self.clear_transaction(session);

        self.drop_temp_items(session.conn_id());
        self.catalog
            .drop_temporary_schema(session.conn_id())
            .expect("unable to drop temporary schema");
        self.active_conns.remove(&session.conn_id());
    }

    /// Handle removing in-progress transaction state regardless of the end action
    /// of the transaction.
    fn clear_transaction(&mut self, session: &mut Session) -> TransactionStatus {
        let (drop_sinks, txn) = session.clear_transaction();
        self.drop_sinks(drop_sinks);

        // Allow compaction of sources from this transaction.
        self.txn_reads.remove(&session.conn_id());

        txn
    }

    /// Removes all temporary items created by the specified connection, though
    /// not the temporary schema itself.
    fn drop_temp_items(&mut self, conn_id: u32) {
        let ops = self.catalog.drop_temp_item_ops(conn_id);
        self.catalog_transact(ops)
            .expect("unable to drop temporary items for conn_id");
    }

    fn drop_sinks(&mut self, dataflow_names: Vec<GlobalId>) {
        if !dataflow_names.is_empty() {
            self.broadcast(dataflow::Command::DropSinks(dataflow_names));
        }
    }

    pub(crate) fn handle_sink_connector_ready(
        &mut self,
        id: GlobalId,
        oid: u32,
        connector: SinkConnector,
    ) -> Result<(), CoordError> {
        // Update catalog entry with sink connector.
        let entry = self.catalog.get_by_id(&id);
        let name = entry.name().clone();
        let mut sink = match entry.item() {
            CatalogItem::Sink(sink) => sink.clone(),
            _ => unreachable!(),
        };
        sink.connector = catalog::SinkConnectorState::Ready(connector.clone());
        let ops = vec![
            catalog::Op::DropItem(id),
            catalog::Op::CreateItem {
                id,
                oid,
                name: name.clone(),
                item: CatalogItem::Sink(sink.clone()),
            },
        ];
        self.catalog_transact(ops)?;
        // TODO(mjibson): Should we pass the timeline to determine_frontier to have
        // sinks start at the linearizability time?
        let as_of = SinkAsOf {
            frontier: self.determine_frontier(sink.from, None),
            strict: !sink.with_snapshot,
        };
        let sink_description = dataflow_types::SinkDesc {
            from: sink.from,
            from_desc: self.catalog.get_by_id(&sink.from).desc().unwrap().clone(),
            connector: connector.clone(),
            envelope: Some(sink.envelope),
            as_of,
        };
        let df =
            self.dataflow_builder()
                .build_sink_dataflow(name.to_string(), id, sink_description);

        // For some sinks, we need to block compaction of each timestamp binding
        // until all sinks that depend on a given source have finished writing out that timestamp.
        // To achieve that, each sink will hold a AntichainToken for all of the sources it depends
        // on, and will advance all of its source dependencies' compaction frontiers as it completes
        // writes.
        if connector.requires_source_compaction_holdback() {
            let mut tokens = Vec::new();

            // Collect AntichainTokens from all of the sources that have them.
            for id in connector.transitive_source_dependencies() {
                if let Some(token) = self.since_handles.get(&id) {
                    tokens.push(token.clone());
                }
            }

            let sink_writes = SinkWrites::new(tokens);
            self.sink_writes.insert(id, sink_writes);
        }
        Ok(self.ship_dataflow(df))
    }

    fn generate_view_ops(
        &mut self,
        session: &Session,
        name: FullName,
        view: View,
        replace: Option<GlobalId>,
        materialize: bool,
    ) -> Result<(Vec<catalog::Op>, Option<GlobalId>), CoordError> {
        self.validate_timeline(view.expr.global_uses())?;

        let mut ops = vec![];

        if let Some(id) = replace {
            ops.extend(self.catalog.drop_items_ops(&[id]));
        }
        let view_id = self.catalog.allocate_id()?;
        let view_oid = self.catalog.allocate_oid()?;
        // Optimize the expression so that we can form an accurately typed description.
        let optimized_expr = self.prep_relation_expr(view.expr, ExprPrepStyle::Static)?;
        let desc = RelationDesc::new(optimized_expr.typ(), view.column_names);
        let view = catalog::View {
            create_sql: view.create_sql,
            optimized_expr,
            desc,
            conn_id: if view.temporary {
                Some(session.conn_id())
            } else {
                None
            },
            depends_on: view.depends_on,
        };
        ops.push(catalog::Op::CreateItem {
            id: view_id,
            oid: view_oid,
            name: name.clone(),
            item: CatalogItem::View(view.clone()),
        });
        let index_id = if materialize {
            let mut index_name = name.clone();
            index_name.item += "_primary_idx";
            index_name = self
                .catalog
                .for_session(session)
                .find_available_name(index_name);
            let index_id = self.catalog.allocate_id()?;
            let index = auto_generate_primary_idx(
                index_name.item.clone(),
                name,
                view_id,
                &view.desc,
                view.conn_id,
                vec![view_id],
                self.catalog.index_enabled_by_default(&index_id),
            );
            let index_oid = self.catalog.allocate_oid()?;
            ops.push(catalog::Op::CreateItem {
                id: index_id,
                oid: index_oid,
                name: index_name,
                item: CatalogItem::Index(index),
            });
            Some(index_id)
        } else {
            None
        };

        Ok((ops, index_id))
    }

    /// Prepares a relation expression for execution by preparing all contained
    /// scalar expressions (see `prep_scalar_expr`), then optimizing the
    /// relation expression.
    fn prep_relation_expr(
        &mut self,
        mut expr: MirRelationExpr,
        style: ExprPrepStyle,
    ) -> Result<OptimizedMirRelationExpr, CoordError> {
        if let ExprPrepStyle::Static = style {
            let mut opt_expr = self.view_optimizer.optimize(expr)?;
            opt_expr.0.try_visit_mut(&mut |e| {
                // Carefully test filter expressions, which may represent temporal filters.
                if let expr::MirRelationExpr::Filter { input, predicates } = &*e {
                    let mfp = expr::MapFilterProject::new(input.arity())
                        .filter(predicates.iter().cloned());
                    match mfp.into_plan() {
                        Err(e) => coord_bail!("{:?}", e),
                        Ok(_) => Ok(()),
                    }
                } else {
                    e.try_visit_scalars_mut1(&mut |s| Self::prep_scalar_expr(s, style))
                }
            })?;
            Ok(opt_expr)
        } else {
            expr.try_visit_scalars_mut(&mut |s| Self::prep_scalar_expr(s, style))?;
            // TODO (wangandi): Is there anything that optimizes to a
            // constant expression that originally contains a global get? Is
            // there anything not containing a global get that cannot be
            // optimized to a constant expression?
            Ok(self.view_optimizer.optimize(expr)?)
        }
    }

    /// Prepares a scalar expression for execution by replacing any placeholders
    /// with their correct values.
    ///
    /// Specifically, calls to the special function `MzLogicalTimestamp` are
    /// replaced if `style` is `OneShot { logical_timestamp }`. Calls are not
    /// replaced for the `Explain` style nor for `Static` which should not
    /// reach this point if we have correctly validated the use of placeholders.
    fn prep_scalar_expr(expr: &mut MirScalarExpr, style: ExprPrepStyle) -> Result<(), CoordError> {
        // Replace calls to `MzLogicalTimestamp` as described above.
        let mut observes_ts = false;
        expr.visit_mut(&mut |e| {
            if let MirScalarExpr::CallNullary(f @ NullaryFunc::MzLogicalTimestamp) = e {
                observes_ts = true;
                if let ExprPrepStyle::OneShot { logical_time } = style {
                    let ts = numeric::Numeric::from(logical_time);
                    *e = MirScalarExpr::literal_ok(Datum::from(ts), f.output_type().scalar_type);
                }
            }
        });
        if observes_ts && matches!(style, ExprPrepStyle::Static | ExprPrepStyle::Write) {
            return Err(CoordError::Unsupported(
                "calls to mz_logical_timestamp in in static or write queries",
            ));
        }
        Ok(())
    }

    fn generate_create_source_ops(
        &mut self,
        session: &mut Session,
        plans: Vec<CreateSourcePlan>,
    ) -> Result<(Vec<(GlobalId, Option<GlobalId>)>, Vec<catalog::Op>), CoordError> {
        let mut metadata = vec![];
        let mut ops = vec![];
        for plan in plans {
            let CreateSourcePlan {
                name,
                source,
                materialized,
                ..
            } = plan;
            let optimized_expr = self.view_optimizer.optimize(source.expr)?;
            let transformed_desc = RelationDesc::new(optimized_expr.0.typ(), source.column_names);
            let source = catalog::Source {
                create_sql: source.create_sql,
                optimized_expr,
                connector: source.connector,
                bare_desc: source.bare_desc,
                desc: transformed_desc,
            };
            let source_id = self.catalog.allocate_id()?;
            let source_oid = self.catalog.allocate_oid()?;
            ops.push(catalog::Op::CreateItem {
                id: source_id,
                oid: source_oid,
                name: name.clone(),
                item: CatalogItem::Source(source.clone()),
            });
            let index_id = if materialized {
                let mut index_name = name.clone();
                index_name.item += "_primary_idx";
                index_name = self
                    .catalog
                    .for_session(session)
                    .find_available_name(index_name);
                let index_id = self.catalog.allocate_id()?;
                let index = auto_generate_primary_idx(
                    index_name.item.clone(),
                    name,
                    source_id,
                    &source.desc,
                    None,
                    vec![source_id],
                    self.catalog.index_enabled_by_default(&index_id),
                );
                let index_oid = self.catalog.allocate_oid()?;
                ops.push(catalog::Op::CreateItem {
                    id: index_id,
                    oid: index_oid,
                    name: index_name,
                    item: CatalogItem::Index(index),
                });
                Some(index_id)
            } else {
                None
            };
            metadata.push((source_id, index_id))
        }
        Ok((metadata, ops))
    }

    /// Attempts to immediately grant `session` access to the write lock or
    /// errors if the lock is currently held.
    fn try_grant_session_write_lock(
        &self,
        session: &mut Session,
    ) -> Result<(), tokio::sync::TryLockError> {
        self.write_lock.clone().try_lock_owned().map(|p| {
            session.grant_write_lock(p);
        })
    }

    /// Defers executing `plan` until the write lock becomes available; waiting
    /// occurs in a greenthread, so callers of this function likely want to
    /// return after calling it.
    fn defer_write(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        session: Session,
        plan: Plan,
    ) {
        let plan = DeferredPlan { tx, session, plan };
        self.write_lock_wait_group.push_back(plan);

        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let write_lock = Arc::clone(&self.write_lock);
        tokio::spawn(async move {
            let guard = write_lock.lock_owned().await;
            internal_cmd_tx
                .send(Message::WriteLockGrant(guard))
                .expect("sending to internal_cmd_tx cannot fail");
        });
    }
}

/// The styles in which an expression can be prepared.
#[derive(Clone, Copy, Debug)]
enum ExprPrepStyle {
    /// The expression is being prepared for output as part of an `EXPLAIN`
    /// query.
    Explain,
    /// The expression is being prepared for installation in a static context,
    /// like in a view.
    Static,
    /// The expression is being prepared to run once at the specified logical
    /// time.
    OneShot { logical_time: u64 },
    /// The expression is being prepared to run in an INSERT or other write.
    Write,
}

/// Constructs an [`ExecuteResponse`] that that will send some rows to the
/// client immediately, as opposed to asking the dataflow layer to send along
/// the rows after some computation.
fn send_immediate_rows(rows: Vec<Row>) -> ExecuteResponse {
    ExecuteResponse::SendingRows(Box::pin(async { PeekResponse::Rows(rows) }))
}

fn auto_generate_primary_idx(
    index_name: String,
    on_name: FullName,
    on_id: GlobalId,
    on_desc: &RelationDesc,
    conn_id: Option<u32>,
    depends_on: Vec<GlobalId>,
    enabled: bool,
) -> catalog::Index {
    let default_key = on_desc.typ().default_key();
    catalog::Index {
        create_sql: index_sql(index_name, on_name, &on_desc, &default_key),
        on: on_id,
        keys: default_key
            .iter()
            .map(|k| MirScalarExpr::Column(*k))
            .collect(),
        conn_id,
        depends_on,
        enabled,
    }
}

// TODO(benesch): constructing the canonical CREATE INDEX statement should be
// the responsibility of the SQL package.
pub(crate) fn index_sql(
    index_name: String,
    view_name: FullName,
    view_desc: &RelationDesc,
    keys: &[usize],
) -> String {
    use sql::ast::{Expr, Value};

    CreateIndexStatement::<Raw> {
        name: Some(Ident::new(index_name)),
        on_name: sql::normalize::unresolve(view_name),
        key_parts: Some(
            keys.iter()
                .map(|i| match view_desc.get_unambiguous_name(*i) {
                    Some(n) => Expr::Identifier(vec![Ident::new(n.to_string())]),
                    _ => Expr::Value(Value::Number((i + 1).to_string())),
                })
                .collect(),
        ),
        with_options: vec![],
        if_not_exists: false,
    }
    .to_ast_string_stable()
}

/// Creates a description of the statement `stmt`.
///
/// This function is identical to sql::plan::describe except this is also
/// supports describing FETCH statements which need access to bound portals
/// through the session.
pub(crate) fn describe(
    catalog: &Catalog,
    stmt: Statement<Raw>,
    param_types: &[Option<pgrepr::Type>],
    session: &Session,
) -> Result<StatementDesc, CoordError> {
    match stmt {
        // FETCH's description depends on the current session, which describe_statement
        // doesn't (and shouldn't?) have access to, so intercept it here.
        Statement::Fetch(FetchStatement { ref name, .. }) => {
            match session.get_portal(name.as_str()).map(|p| p.desc.clone()) {
                Some(desc) => Ok(desc),
                None => Err(CoordError::UnknownCursor(name.to_string())),
            }
        }
        _ => {
            let catalog = &catalog.for_session(session);
            Ok(sql::plan::describe(
                &session.pcx(),
                catalog,
                stmt,
                param_types,
            )?)
        }
    }
}

fn check_statement_safety(stmt: &Statement<Raw>) -> Result<(), CoordError> {
    let (source_or_sink, typ, with_options) = match stmt {
        Statement::CreateSource(CreateSourceStatement {
            connector,
            with_options,
            ..
        }) => ("source", ConnectorType::from(connector), with_options),
        Statement::CreateSink(CreateSinkStatement {
            connector,
            with_options,
            ..
        }) => ("sink", ConnectorType::from(connector), with_options),
        _ => return Ok(()),
    };
    match typ {
        // File sources and sinks are prohibited in safe mode because they allow
        // reading ƒrom and writing to arbitrary files on disk.
        ConnectorType::File => {
            return Err(CoordError::SafeModeViolation(format!(
                "file {}",
                source_or_sink
            )));
        }
        ConnectorType::AvroOcf => {
            return Err(CoordError::SafeModeViolation(format!(
                "Avro OCF {}",
                source_or_sink
            )));
        }
        // Kerberos-authenticated Kafka sources and sinks are prohibited in
        // safe mode because librdkafka will blindly execute the string passed
        // as `sasl_kerberos_kinit_cmd`.
        ConnectorType::Kafka => {
            // It's too bad that we have to reinvent so much of librdkafka's
            // option parsing and hardcode some of its defaults here. But there
            // isn't an obvious alternative; asking librdkafka about its =
            // defaults requires constructing a librdkafka client, and at that
            // point it's already too late.
            let mut with_options = sql::normalize::options(with_options);
            let with_options = sql::kafka_util::extract_config(&mut with_options)?;
            let security_protocol = with_options
                .get("security.protocol")
                .map(|v| v.as_str())
                .unwrap_or("plaintext");
            let sasl_mechanism = with_options
                .get("sasl.mechanisms")
                .map(|v| v.as_str())
                .unwrap_or("GSSAPI");
            if (security_protocol.eq_ignore_ascii_case("sasl_plaintext")
                || security_protocol.eq_ignore_ascii_case("sasl_ssl"))
                && sasl_mechanism.eq_ignore_ascii_case("GSSAPI")
            {
                return Err(CoordError::SafeModeViolation(format!(
                    "Kerberos-authenticated Kafka {}",
                    source_or_sink,
                )));
            }
        }
        _ => (),
    }
    Ok(())
}

/// Logic and types for fast-path determination for dataflow execution.
///
/// This module determines if a dataflow can be short-cut, by returning constant values
/// or by reading out of existing arrangements, and implements the appropriate plan.
pub mod fast_path_peek {

    use crate::CoordError;
    use expr::{EvalError, GlobalId, Id};
    use repr::{Diff, Row};

    /// Possible ways in which the coordinator could produce the result for a goal view.
    #[derive(Debug)]
    pub enum Plan {
        /// The view evaluates to a constant result that can be returned.
        Constant(Result<Vec<(Row, repr::Timestamp, Diff)>, EvalError>),
        /// The view can be read out of an existing arrangement.
        PeekExisting(GlobalId, Option<Row>, expr::SafeMfpPlan),
        /// The view must be installed as a dataflow and then read.
        PeekDataflow(
            dataflow_types::DataflowDescription<dataflow::Plan>,
            GlobalId,
        ),
    }

    /// Determine if the dataflow plan can be implemented without an actual dataflow.
    ///
    /// If the optimized plan is a `Constant` or a `Get` of a maintained arrangement,
    /// we can avoid building a dataflow (and either just return the results, or peek
    /// out of the arrangement, respectively).
    pub(crate) fn create_plan(
        dataflow_plan: dataflow_types::DataflowDescription<dataflow::Plan>,
        view_id: GlobalId,
        index_id: GlobalId,
    ) -> Result<Plan, CoordError> {
        // At this point, `dataflow_plan` contains our best optimized dataflow.
        // We will check the plan to see if there is a fast path to escape full dataflow construction.

        // We need to restrict ourselves to settings where the inserted transient view is the first thing
        // to build (no dependent views). There is likely an index to build as well, but we may not be sure.
        if dataflow_plan.objects_to_build.len() >= 1
            && dataflow_plan.objects_to_build[0].id == view_id
        {
            match &dataflow_plan.objects_to_build[0].view {
                // In the case of a constant, we can return the result now.
                dataflow::Plan::Constant { rows } => {
                    return Ok(Plan::Constant(rows.clone()));
                }
                // In the case of a bare `Get`, we may be able to directly index an arrangement.
                dataflow::Plan::Get {
                    id,
                    keys: _,
                    mfp,
                    key_val,
                } => {
                    // Convert `mfp` to an executable, non-temporal plan.
                    // It should be non-temporal, as OneShot preparation populates `mz_logical_timestamp`.
                    let map_filter_project = mfp
                        .clone()
                        .into_plan()
                        .map_err(|e| crate::error::CoordError::Unstructured(::anyhow::anyhow!(e)))?
                        .into_nontemporal()
                        .map_err(|_e| {
                            crate::error::CoordError::Unstructured(::anyhow::anyhow!(
                                "OneShot plan has temporal constraints"
                            ))
                        })?;
                    // We should only get excited if we can track down an index for `id`.
                    // If `keys` is non-empty, that means we think one exists.
                    for (index_id, (desc, _typ)) in dataflow_plan.index_imports.iter() {
                        if let Some((key, val)) = key_val {
                            if Id::Global(desc.on_id) == *id && &desc.keys == key {
                                // Indicate an early exit with a specific index and key_val.
                                return Ok(Plan::PeekExisting(
                                    *index_id,
                                    Some(val.clone()),
                                    map_filter_project,
                                ));
                            }
                        } else if Id::Global(desc.on_id) == *id {
                            // Indicate an early exit with a specific index and no key_val.
                            return Ok(Plan::PeekExisting(*index_id, None, map_filter_project));
                        }
                    }
                }
                // nothing can be done for non-trivial expressions.
                _ => {}
            }
        }
        return Ok(Plan::PeekDataflow(dataflow_plan, index_id));
    }

    impl crate::coord::Coordinator {
        /// Implements a peek plan produced by `create_plan` above.
        pub(crate) fn implement_fast_path_peek(
            &mut self,
            fast_path: Plan,
            timestamp: repr::Timestamp,
            finishing: expr::RowSetFinishing,
            conn_id: u32,
            source_arity: usize,
        ) -> Result<crate::ExecuteResponse, CoordError> {
            // If the dataflow optimizes to a constant expression, we can immediately return the result.
            if let Plan::Constant(rows) = fast_path {
                let mut rows = match rows {
                    Ok(rows) => rows,
                    Err(e) => return Err(e.into()),
                };
                // retain exactly those updates less or equal to `timestamp`.
                for (_, time, diff) in rows.iter_mut() {
                    use timely::PartialOrder;
                    if time.less_equal(&timestamp) {
                        // clobber the timestamp, so consolidation occurs.
                        *time = timestamp.clone();
                    } else {
                        // zero the difference, to prevent a contribution.
                        *diff = 0;
                    }
                }
                // Consolidate down the results to get correct totals.
                differential_dataflow::consolidation::consolidate_updates(&mut rows);

                let mut results = Vec::new();
                for (ref row, _time, count) in rows {
                    if count < 0 {
                        Err(EvalError::InvalidParameterValue(format!(
                            "Negative multiplicity in constant result: {}",
                            count
                        )))?
                    };
                    for _ in 0..count {
                        // TODO: If `count` is too large, or `results` too full, we could error.
                        results.push(row.clone());
                    }
                }
                finishing.finish(&mut results);
                return Ok(crate::handle::send_immediate_rows(results));
            }

            // The remaining cases are a peek into a maintained arrangement, or building a dataflow.
            // In both cases we will want to peek, and the main difference is that we might want to
            // build a dataflow and drop it once the peek is issued. The peeks are also constructed
            // differently.

            // If we must build the view, ship the dataflow.
            let (peek_command, drop_dataflow) = match fast_path {
                Plan::PeekExisting(id, key, map_filter_project) => (
                    dataflow::Command::Peek {
                        id,
                        key,
                        conn_id,
                        timestamp,
                        finishing: finishing.clone(),
                        map_filter_project,
                    },
                    None,
                ),
                Plan::PeekDataflow(dataflow, index_id) => {
                    // Very important: actually create the dataflow (here, so we can destructure).
                    self.broadcast(dataflow::Command::CreateDataflows(vec![dataflow]));

                    // Create an identity MFP operator.
                    let map_filter_project = expr::MapFilterProject::new(source_arity)
                        .into_plan()
                        .map_err(|e| crate::error::CoordError::Unstructured(::anyhow::anyhow!(e)))?
                        .into_nontemporal()
                        .map_err(|_e| {
                            crate::error::CoordError::Unstructured(::anyhow::anyhow!(
                                "OneShot plan has temporal constraints"
                            ))
                        })?;
                    (
                        dataflow::Command::Peek {
                            id: index_id, // transient identifier produced by `dataflow_plan`.
                            key: None,
                            conn_id,
                            timestamp,
                            finishing: finishing.clone(),
                            map_filter_project,
                        },
                        Some(index_id),
                    )
                }
                _ => {
                    unreachable!()
                }
            };

            // Endpoints for sending and receiving peek responses.
            let (rows_tx, rows_rx) = tokio::sync::mpsc::unbounded_channel();

            // The peek is ready to go for both cases, fast and non-fast.
            // Stash the response mechanism, and broadcast dataflow construction.
            self.pending_peeks
                .insert(conn_id, (rows_tx, std::collections::HashSet::new()));
            self.broadcast(peek_command);

            use dataflow_types::PeekResponse;
            use futures::FutureExt;
            use futures::StreamExt;

            // Prepare the receiver to return as a response.
            let rows_rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rows_rx)
                .fold(PeekResponse::Rows(vec![]), |memo, resp| async {
                    match (memo, resp) {
                        (PeekResponse::Rows(mut memo), PeekResponse::Rows(rows)) => {
                            memo.extend(rows);
                            PeekResponse::Rows(memo)
                        }
                        (PeekResponse::Error(e), _) | (_, PeekResponse::Error(e)) => {
                            PeekResponse::Error(e)
                        }
                        (PeekResponse::Canceled, _) | (_, PeekResponse::Canceled) => {
                            PeekResponse::Canceled
                        }
                    }
                })
                .map(move |mut resp| {
                    if let PeekResponse::Rows(rows) = &mut resp {
                        finishing.finish(rows)
                    }
                    resp
                });

            // If it was created, drop the dataflow once the peek command is sent.
            if let Some(index_id) = drop_dataflow {
                self.drop_indexes(vec![index_id]);
            }

            Ok(crate::ExecuteResponse::SendingRows(Box::pin(rows_rx)))
        }
    }
}
