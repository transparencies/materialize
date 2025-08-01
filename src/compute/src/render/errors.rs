// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers for handling errors encountered by operators.

use mz_repr::Row;

use crate::render::context::ShutdownProbe;

/// Used to make possibly-validating code generic: think of this as a kind of `MaybeResult`,
/// specialized for use in compute.  Validation code will only run when the error constructor is
/// Some.
pub(super) trait MaybeValidatingRow<T, E> {
    fn ok(t: T) -> Self;
    fn into_error() -> Option<fn(E) -> Self>;
}

impl<E> MaybeValidatingRow<Row, E> for Row {
    fn ok(t: Row) -> Self {
        t
    }

    fn into_error() -> Option<fn(E) -> Self> {
        None
    }
}

impl<E> MaybeValidatingRow<(), E> for () {
    fn ok(t: ()) -> Self {
        t
    }

    fn into_error() -> Option<fn(E) -> Self> {
        None
    }
}

impl<E, R> MaybeValidatingRow<Vec<R>, E> for Vec<R> {
    fn ok(t: Vec<R>) -> Self {
        t
    }

    fn into_error() -> Option<fn(E) -> Self> {
        None
    }
}

impl<T, E> MaybeValidatingRow<T, E> for Result<T, E> {
    fn ok(row: T) -> Self {
        Ok(row)
    }

    fn into_error() -> Option<fn(E) -> Self> {
        Some(Err)
    }
}

/// Error logger to be used by rendering code.
///
/// Holds onto a `[ShutdownProbe`] to ensure that no false-positive errors are logged while the
/// dataflow is in the process of shutting down.
#[derive(Clone)]
pub(super) struct ErrorLogger {
    shutdown_probe: ShutdownProbe,
    dataflow_name: String,
}

impl ErrorLogger {
    pub fn new(shutdown_probe: ShutdownProbe, dataflow_name: String) -> Self {
        Self {
            shutdown_probe,
            dataflow_name,
        }
    }

    /// Log the given error, unless the dataflow is shutting down.
    ///
    /// The logging format is optimized for surfacing errors with Sentry:
    ///  * `error` is logged at ERROR level and will appear as the error title in Sentry.
    ///    We require it to be a static string, to ensure that Sentry always merges instances of
    ///    the same error together.
    ///  * `details` is logged at WARN level and will appear in the breadcrumbs.
    ///    Put relevant dynamic information here.
    ///
    /// The message that's logged at WARN level has the format
    ///   "[customer-data] {message} ({details})"
    /// We include the [customer-data] tag out of the expectation that `details` will always
    /// contain some sensitive customer data. We include the `message` to make it possible to match
    /// the breadcrumbs to their associated error in Sentry.
    ///
    // TODO(database-issues#5362): Rethink or justify our error logging strategy.
    pub fn log(&self, message: &'static str, details: &str) {
        // It's important that we silence errors as soon as the local shutdown token has been
        // dropped. Dataflow operators may start discarding results, thereby producing incorrect
        // output, as soon as they observe that all workers have dropped their token. However, not
        // all workers are guaranteed to make this observation at the same time. So it's possible
        // that some workers have already started discarding results while other workers still see
        // `shutdown_probe.in_shutdown() == false`.
        if !self.shutdown_probe.in_local_shutdown() {
            self.log_always(message, details);
        }
    }

    /// Like [`Self::log`], but also logs errors when the dataflow is shutting down.
    ///
    /// Use this method to notify about errors that cannot be caused by dataflow shutdown.
    pub fn log_always(&self, message: &'static str, details: &str) {
        tracing::warn!(
            dataflow = self.dataflow_name,
            "[customer-data] {message} ({details})"
        );
        tracing::error!(message);
    }

    /// Like [`Self::log_always`], but panics in debug mode.
    ///
    /// Use this method to notify about errors that are certainly caused by bugs in Materialize.
    pub fn soft_panic_or_log(&self, message: &'static str, details: &str) {
        tracing::warn!(
            dataflow = self.dataflow_name,
            "[customer-data] {message} ({details})"
        );
        mz_ore::soft_panic_or_log!("{}", message);
    }
}
