// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_ore::assert_none;
use mz_ore::cast::CastFrom;
use mz_persist::location::{SeqNo, VersionedData};
use mz_persist_types::Codec64;
use mz_persist_types::schema::SchemaId;
use mz_proto::TryFromProtoError;
use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp};
use tracing::debug;

use crate::critical::CriticalReaderId;
use crate::internal::paths::PartialRollupKey;
use crate::internal::state::{
    CriticalReaderState, EncodedSchemas, HollowBatch, HollowBlobRef, HollowRollup,
    LeasedReaderState, ProtoStateField, ProtoStateFieldDiffType, ProtoStateFieldDiffs, RunPart,
    State, StateCollections, WriterState,
};
use crate::internal::trace::CompactionInput;
use crate::internal::trace::{FueledMergeRes, SpineId, ThinMerge, ThinSpineBatch, Trace};
use crate::read::LeasedReaderId;
use crate::write::WriterId;
use crate::{Metrics, PersistConfig, ShardId};

use StateFieldValDiff::*;

use super::state::{ActiveGc, ActiveRollup};

#[derive(Clone, Debug)]
#[cfg_attr(any(test, debug_assertions), derive(PartialEq))]
pub enum StateFieldValDiff<V> {
    Insert(V),
    Update(V, V),
    Delete(V),
}

#[derive(Clone)]
#[cfg_attr(any(test, debug_assertions), derive(PartialEq))]
pub struct StateFieldDiff<K, V> {
    pub key: K,
    pub val: StateFieldValDiff<V>,
}

impl<K: Debug, V: Debug> std::fmt::Debug for StateFieldDiff<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateFieldDiff")
            // In the cases we've seen in the wild, it's been more useful to
            // have the val printed first.
            .field("val", &self.val)
            .field("key", &self.key)
            .finish()
    }
}

#[derive(Debug)]
#[cfg_attr(any(test, debug_assertions), derive(Clone, PartialEq))]
pub struct StateDiff<T> {
    pub(crate) applier_version: semver::Version,
    pub(crate) seqno_from: SeqNo,
    pub(crate) seqno_to: SeqNo,
    pub(crate) walltime_ms: u64,
    pub(crate) latest_rollup_key: PartialRollupKey,
    pub(crate) rollups: Vec<StateFieldDiff<SeqNo, HollowRollup>>,
    pub(crate) active_rollup: Vec<StateFieldDiff<(), ActiveRollup>>,
    pub(crate) active_gc: Vec<StateFieldDiff<(), ActiveGc>>,
    pub(crate) hostname: Vec<StateFieldDiff<(), String>>,
    pub(crate) last_gc_req: Vec<StateFieldDiff<(), SeqNo>>,
    pub(crate) leased_readers: Vec<StateFieldDiff<LeasedReaderId, LeasedReaderState<T>>>,
    pub(crate) critical_readers: Vec<StateFieldDiff<CriticalReaderId, CriticalReaderState<T>>>,
    pub(crate) writers: Vec<StateFieldDiff<WriterId, WriterState<T>>>,
    pub(crate) schemas: Vec<StateFieldDiff<SchemaId, EncodedSchemas>>,
    pub(crate) since: Vec<StateFieldDiff<(), Antichain<T>>>,
    pub(crate) legacy_batches: Vec<StateFieldDiff<HollowBatch<T>, ()>>,
    pub(crate) hollow_batches: Vec<StateFieldDiff<SpineId, Arc<HollowBatch<T>>>>,
    pub(crate) spine_batches: Vec<StateFieldDiff<SpineId, ThinSpineBatch<T>>>,
    pub(crate) merges: Vec<StateFieldDiff<SpineId, ThinMerge<T>>>,
}

impl<T: Timestamp + Codec64> StateDiff<T> {
    pub fn new(
        applier_version: semver::Version,
        seqno_from: SeqNo,
        seqno_to: SeqNo,
        walltime_ms: u64,
        latest_rollup_key: PartialRollupKey,
    ) -> Self {
        StateDiff {
            applier_version,
            seqno_from,
            seqno_to,
            walltime_ms,
            latest_rollup_key,
            rollups: Vec::default(),
            active_rollup: Vec::default(),
            active_gc: Vec::default(),
            hostname: Vec::default(),
            last_gc_req: Vec::default(),
            leased_readers: Vec::default(),
            critical_readers: Vec::default(),
            writers: Vec::default(),
            schemas: Vec::default(),
            since: Vec::default(),
            legacy_batches: Vec::default(),
            hollow_batches: Vec::default(),
            spine_batches: Vec::default(),
            merges: Vec::default(),
        }
    }

    pub fn referenced_batches(&self) -> impl Iterator<Item = StateFieldValDiff<&HollowBatch<T>>> {
        let legacy_batches = self
            .legacy_batches
            .iter()
            .filter_map(|diff| match diff.val {
                Insert(()) => Some(Insert(&diff.key)),
                Update((), ()) => None, // Ignoring a noop diff.
                Delete(()) => Some(Delete(&diff.key)),
            });
        let hollow_batches = self.hollow_batches.iter().map(|diff| match &diff.val {
            Insert(batch) => Insert(&**batch),
            Update(before, after) => Update(&**before, &**after),
            Delete(batch) => Delete(&**batch),
        });
        legacy_batches.chain(hollow_batches)
    }
}

impl<T: Timestamp + Lattice + Codec64> StateDiff<T> {
    pub fn from_diff(from: &State<T>, to: &State<T>) -> Self {
        // Deconstruct from and to so we get a compile failure if new
        // fields are added.
        let State {
            applier_version: _,
            shard_id: from_shard_id,
            seqno: from_seqno,
            hostname: from_hostname,
            walltime_ms: _, // Intentionally unused
            collections:
                StateCollections {
                    last_gc_req: from_last_gc_req,
                    rollups: from_rollups,
                    active_rollup: from_active_rollup,
                    active_gc: from_active_gc,
                    leased_readers: from_leased_readers,
                    critical_readers: from_critical_readers,
                    writers: from_writers,
                    schemas: from_schemas,
                    trace: from_trace,
                },
        } = from;
        let State {
            applier_version: to_applier_version,
            shard_id: to_shard_id,
            seqno: to_seqno,
            walltime_ms: to_walltime_ms,
            hostname: to_hostname,
            collections:
                StateCollections {
                    last_gc_req: to_last_gc_req,
                    rollups: to_rollups,
                    active_rollup: to_active_rollup,
                    active_gc: to_active_gc,
                    leased_readers: to_leased_readers,
                    critical_readers: to_critical_readers,
                    writers: to_writers,
                    schemas: to_schemas,
                    trace: to_trace,
                },
        } = to;
        assert_eq!(from_shard_id, to_shard_id);

        let (_, latest_rollup) = to.latest_rollup();
        let mut diffs = Self::new(
            to_applier_version.clone(),
            *from_seqno,
            *to_seqno,
            *to_walltime_ms,
            latest_rollup.key.clone(),
        );
        diff_field_single(from_hostname, to_hostname, &mut diffs.hostname);
        diff_field_single(from_last_gc_req, to_last_gc_req, &mut diffs.last_gc_req);
        diff_field_sorted_iter(
            from_active_rollup.iter().map(|r| (&(), r)),
            to_active_rollup.iter().map(|r| (&(), r)),
            &mut diffs.active_rollup,
        );
        diff_field_sorted_iter(
            from_active_gc.iter().map(|g| (&(), g)),
            to_active_gc.iter().map(|g| (&(), g)),
            &mut diffs.active_gc,
        );
        diff_field_sorted_iter(from_rollups.iter(), to_rollups, &mut diffs.rollups);
        diff_field_sorted_iter(
            from_leased_readers.iter(),
            to_leased_readers,
            &mut diffs.leased_readers,
        );
        diff_field_sorted_iter(
            from_critical_readers.iter(),
            to_critical_readers,
            &mut diffs.critical_readers,
        );
        diff_field_sorted_iter(from_writers.iter(), to_writers, &mut diffs.writers);
        diff_field_sorted_iter(from_schemas.iter(), to_schemas, &mut diffs.schemas);
        diff_field_single(from_trace.since(), to_trace.since(), &mut diffs.since);

        let from_flat = from_trace.flatten();
        let to_flat = to_trace.flatten();
        diff_field_sorted_iter(
            from_flat.legacy_batches.iter().map(|(k, v)| (&**k, v)),
            to_flat.legacy_batches.iter().map(|(k, v)| (&**k, v)),
            &mut diffs.legacy_batches,
        );
        diff_field_sorted_iter(
            from_flat.hollow_batches.iter(),
            to_flat.hollow_batches.iter(),
            &mut diffs.hollow_batches,
        );
        diff_field_sorted_iter(
            from_flat.spine_batches.iter(),
            to_flat.spine_batches.iter(),
            &mut diffs.spine_batches,
        );
        diff_field_sorted_iter(
            from_flat.merges.iter(),
            to_flat.merges.iter(),
            &mut diffs.merges,
        );
        diffs
    }

    pub(crate) fn blob_inserts(&self) -> impl Iterator<Item = HollowBlobRef<T>> {
        let batches = self
            .referenced_batches()
            .filter_map(|spine_diff| match spine_diff {
                Insert(b) | Update(_, b) => Some(HollowBlobRef::Batch(b)),
                Delete(_) => None, // No-op
            });
        let rollups = self
            .rollups
            .iter()
            .filter_map(|rollups_diff| match &rollups_diff.val {
                StateFieldValDiff::Insert(x) | StateFieldValDiff::Update(_, x) => {
                    Some(HollowBlobRef::Rollup(x))
                }
                StateFieldValDiff::Delete(_) => None, // No-op
            });
        batches.chain(rollups)
    }

    pub(crate) fn part_deletes(&self) -> impl Iterator<Item = &RunPart<T>> {
        // With the introduction of incremental compaction, we
        // need to be more careful about what we consider "deleted".
        // If there is a HollowBatch that we replace 2 out of the 4 runs of,
        // we need to ensure that we only delete the runs that are actually
        // no longer referenced.
        let removed = self
            .referenced_batches()
            .filter_map(|spine_diff| match spine_diff {
                Insert(_) => None,
                Update(a, _) | Delete(a) => Some(a.parts.iter().collect::<Vec<_>>()),
            });

        let added: std::collections::BTreeSet<_> = self
            .referenced_batches()
            .filter_map(|spine_diff| match spine_diff {
                Insert(a) | Update(_, a) => Some(a.parts.iter().collect::<Vec<_>>()),
                Delete(_) => None,
            })
            .flatten()
            .collect();

        removed
            .into_iter()
            .flat_map(|x| x)
            .filter(move |part| !added.contains(part))
    }

    pub(crate) fn rollup_deletes(&self) -> impl Iterator<Item = &HollowRollup> {
        self.rollups
            .iter()
            .filter_map(|rollups_diff| match &rollups_diff.val {
                Insert(_) => None,
                Update(a, _) | Delete(a) => Some(a),
            })
    }

    #[cfg(any(test, debug_assertions))]
    #[allow(dead_code)]
    pub fn validate_roundtrip<K, V, D>(
        metrics: &Metrics,
        from_state: &crate::internal::state::TypedState<K, V, T, D>,
        diff: &Self,
        to_state: &crate::internal::state::TypedState<K, V, T, D>,
    ) -> Result<(), String>
    where
        K: mz_persist_types::Codec + std::fmt::Debug,
        V: mz_persist_types::Codec + std::fmt::Debug,
        D: differential_dataflow::difference::Semigroup + Codec64,
    {
        use mz_proto::RustType;
        use prost::Message;

        use crate::internal::state::ProtoStateDiff;

        let mut roundtrip_state = from_state.clone(
            from_state.applier_version.clone(),
            from_state.hostname.clone(),
        );
        roundtrip_state.apply_diff(metrics, diff.clone())?;

        if &roundtrip_state != to_state {
            // The weird spacing in this format string is so they all line up
            // when printed out.
            return Err(format!(
                "state didn't roundtrip\n  from_state {:?}\n  to_state   {:?}\n  rt_state   {:?}\n  diff       {:?}\n",
                from_state, to_state, roundtrip_state, diff
            ));
        }

        let encoded_diff = diff.into_proto().encode_to_vec();
        let roundtrip_diff = Self::from_proto(
            ProtoStateDiff::decode(encoded_diff.as_slice()).map_err(|err| err.to_string())?,
        )
        .map_err(|err| err.to_string())?;

        if &roundtrip_diff != diff {
            // The weird spacing in this format string is so they all line up
            // when printed out.
            return Err(format!(
                "diff didn't roundtrip\n  diff    {:?}\n  rt_diff {:?}",
                diff, roundtrip_diff
            ));
        }

        Ok(())
    }
}

impl<T: Timestamp + Lattice + Codec64> State<T> {
    pub fn apply_encoded_diffs<'a, I: IntoIterator<Item = &'a VersionedData>>(
        &mut self,
        cfg: &PersistConfig,
        metrics: &Metrics,
        diffs: I,
    ) {
        let mut state_seqno = self.seqno;
        let diffs = diffs.into_iter().filter_map(move |x| {
            if x.seqno != state_seqno.next() {
                // No-op.
                return None;
            }
            let data = x.data.clone();
            let diff = metrics
                .codecs
                .state_diff
                // Note: `x.data` is a `Bytes`, so cloning just increments a ref count
                .decode(|| StateDiff::decode(&cfg.build_version, x.data.clone()));
            assert_eq!(diff.seqno_from, state_seqno);
            state_seqno = diff.seqno_to;
            Some((diff, data))
        });
        self.apply_diffs(metrics, diffs);
    }
}

impl<T: Timestamp + Lattice + Codec64> State<T> {
    pub fn apply_diffs<I: IntoIterator<Item = (StateDiff<T>, Bytes)>>(
        &mut self,
        metrics: &Metrics,
        diffs: I,
    ) {
        for (diff, data) in diffs {
            // TODO: This could special-case batch apply for diffs where it's
            // more efficient (in particular, spine batches that hit the slow
            // path).
            match self.apply_diff(metrics, diff) {
                Ok(()) => {}
                Err(err) => {
                    // Having the full diff in the error message is critical for debugging any
                    // issues that may arise from diff application. We pass along the original
                    // Bytes it decoded from just so we can decode in this error path, while
                    // avoiding any extraneous clones in the expected Ok path.
                    let diff = StateDiff::<T>::decode(&self.applier_version, data);
                    panic!(
                        "state diff should apply cleanly: {} diff {:?} state {:?}",
                        err, diff, self
                    )
                }
            }
        }
    }

    // Intentionally not even pub(crate) because all callers should use
    // [Self::apply_diffs].
    pub(super) fn apply_diff(
        &mut self,
        metrics: &Metrics,
        diff: StateDiff<T>,
    ) -> Result<(), String> {
        // Deconstruct diff so we get a compile failure if new fields are added.
        let StateDiff {
            applier_version: diff_applier_version,
            seqno_from: diff_seqno_from,
            seqno_to: diff_seqno_to,
            walltime_ms: diff_walltime_ms,
            latest_rollup_key: _,
            rollups: diff_rollups,
            active_rollup: diff_active_rollup,
            active_gc: diff_active_gc,
            hostname: diff_hostname,
            last_gc_req: diff_last_gc_req,
            leased_readers: diff_leased_readers,
            critical_readers: diff_critical_readers,
            writers: diff_writers,
            schemas: diff_schemas,
            since: diff_since,
            legacy_batches: diff_legacy_batches,
            hollow_batches: diff_hollow_batches,
            spine_batches: diff_spine_batches,
            merges: diff_merges,
        } = diff;
        if self.seqno == diff_seqno_to {
            return Ok(());
        }
        if self.seqno != diff_seqno_from {
            return Err(format!(
                "could not apply diff {} -> {} to state {}",
                diff_seqno_from, diff_seqno_to, self.seqno
            ));
        }
        self.seqno = diff_seqno_to;
        self.applier_version = diff_applier_version;
        self.walltime_ms = diff_walltime_ms;
        force_apply_diffs_single(
            &self.shard_id,
            diff_seqno_to,
            "hostname",
            diff_hostname,
            &mut self.hostname,
            metrics,
        )?;

        // Deconstruct collections so we get a compile failure if new fields are
        // added.
        let StateCollections {
            last_gc_req,
            rollups,
            active_rollup,
            active_gc,
            leased_readers,
            critical_readers,
            writers,
            schemas,
            trace,
        } = &mut self.collections;

        apply_diffs_map("rollups", diff_rollups, rollups)?;
        apply_diffs_single("last_gc_req", diff_last_gc_req, last_gc_req)?;
        apply_diffs_single_option("active_rollup", diff_active_rollup, active_rollup)?;
        apply_diffs_single_option("active_gc", diff_active_gc, active_gc)?;
        apply_diffs_map("leased_readers", diff_leased_readers, leased_readers)?;
        apply_diffs_map("critical_readers", diff_critical_readers, critical_readers)?;
        apply_diffs_map("writers", diff_writers, writers)?;
        apply_diffs_map("schemas", diff_schemas, schemas)?;

        let structure_unchanged = diff_hollow_batches.is_empty()
            && diff_spine_batches.is_empty()
            && diff_merges.is_empty();
        let spine_unchanged =
            diff_since.is_empty() && diff_legacy_batches.is_empty() && structure_unchanged;

        if spine_unchanged {
            return Ok(());
        }

        let mut flat = if trace.roundtrip_structure {
            metrics.state.apply_spine_flattened.inc();
            let mut flat = trace.flatten();
            apply_diffs_single("since", diff_since, &mut flat.since)?;
            apply_diffs_map(
                "legacy_batches",
                diff_legacy_batches
                    .into_iter()
                    .map(|StateFieldDiff { key, val }| StateFieldDiff {
                        key: Arc::new(key),
                        val,
                    }),
                &mut flat.legacy_batches,
            )?;
            Some(flat)
        } else {
            for x in diff_since {
                match x.val {
                    Update(from, to) => {
                        if trace.since() != &from {
                            return Err(format!(
                                "since update didn't match: {:?} vs {:?}",
                                self.collections.trace.since(),
                                &from
                            ));
                        }
                        trace.downgrade_since(&to);
                    }
                    Insert(_) => return Err("cannot insert since field".to_string()),
                    Delete(_) => return Err("cannot delete since field".to_string()),
                }
            }
            if !diff_legacy_batches.is_empty() {
                apply_diffs_spine(metrics, diff_legacy_batches, trace)?;
                debug_assert_eq!(trace.validate(), Ok(()), "{:?}", trace);
            }
            None
        };

        if !structure_unchanged {
            let flat = flat.get_or_insert_with(|| trace.flatten());
            apply_diffs_map(
                "hollow_batches",
                diff_hollow_batches,
                &mut flat.hollow_batches,
            )?;
            apply_diffs_map("spine_batches", diff_spine_batches, &mut flat.spine_batches)?;
            apply_diffs_map("merges", diff_merges, &mut flat.merges)?;
        }

        if let Some(flat) = flat {
            *trace = Trace::unflatten(flat)?;
        }

        // There's various sanity checks that this method could run (e.g. since,
        // upper, seqno_since, etc don't regress or that diff.latest_rollup ==
        // state.rollups.last()), are they a good idea? On one hand, I like
        // sanity checks, other the other, one of the goals here is to keep
        // apply logic as straightforward and unchanging as possible.
        Ok(())
    }
}

fn diff_field_single<T: PartialEq + Clone>(
    from: &T,
    to: &T,
    diffs: &mut Vec<StateFieldDiff<(), T>>,
) {
    // This could use the `diff_field_sorted_iter(once(from), once(to), diffs)`
    // general impl, but we just do the obvious thing.
    if from != to {
        diffs.push(StateFieldDiff {
            key: (),
            val: Update(from.clone(), to.clone()),
        })
    }
}

fn apply_diffs_single_option<X: PartialEq + Debug>(
    name: &str,
    diffs: Vec<StateFieldDiff<(), X>>,
    single: &mut Option<X>,
) -> Result<(), String> {
    for diff in diffs {
        apply_diff_single_option(name, diff, single)?;
    }
    Ok(())
}

fn apply_diff_single_option<X: PartialEq + Debug>(
    name: &str,
    diff: StateFieldDiff<(), X>,
    single: &mut Option<X>,
) -> Result<(), String> {
    match diff.val {
        Update(from, to) => {
            if single.as_ref() != Some(&from) {
                return Err(format!(
                    "{} update didn't match: {:?} vs {:?}",
                    name, single, &from
                ));
            }
            *single = Some(to)
        }
        Insert(to) => {
            if single.is_some() {
                return Err(format!("{} insert found existing value", name));
            }
            *single = Some(to)
        }
        Delete(from) => {
            if single.as_ref() != Some(&from) {
                return Err(format!(
                    "{} delete didn't match: {:?} vs {:?}",
                    name, single, &from
                ));
            }
            *single = None
        }
    }
    Ok(())
}

fn apply_diffs_single<X: PartialEq + Debug>(
    name: &str,
    diffs: Vec<StateFieldDiff<(), X>>,
    single: &mut X,
) -> Result<(), String> {
    for diff in diffs {
        apply_diff_single(name, diff, single)?;
    }
    Ok(())
}

fn apply_diff_single<X: PartialEq + Debug>(
    name: &str,
    diff: StateFieldDiff<(), X>,
    single: &mut X,
) -> Result<(), String> {
    match diff.val {
        Update(from, to) => {
            if single != &from {
                return Err(format!(
                    "{} update didn't match: {:?} vs {:?}",
                    name, single, &from
                ));
            }
            *single = to
        }
        Insert(_) => return Err(format!("cannot insert {} field", name)),
        Delete(_) => return Err(format!("cannot delete {} field", name)),
    }
    Ok(())
}

// A hack to force apply a diff, making `single` equal to
// the Update `to` value, ignoring a mismatch on `from`.
// Used to migrate forward after writing down incorrect
// diffs.
//
// TODO: delete this once `hostname` has zero mismatches
fn force_apply_diffs_single<X: PartialEq + Debug>(
    shard_id: &ShardId,
    seqno: SeqNo,
    name: &str,
    diffs: Vec<StateFieldDiff<(), X>>,
    single: &mut X,
    metrics: &Metrics,
) -> Result<(), String> {
    for diff in diffs {
        force_apply_diff_single(shard_id, seqno, name, diff, single, metrics)?;
    }
    Ok(())
}

fn force_apply_diff_single<X: PartialEq + Debug>(
    shard_id: &ShardId,
    seqno: SeqNo,
    name: &str,
    diff: StateFieldDiff<(), X>,
    single: &mut X,
    metrics: &Metrics,
) -> Result<(), String> {
    match diff.val {
        Update(from, to) => {
            if single != &from {
                debug!(
                    "{}: update didn't match: {:?} vs {:?}, continuing to force apply diff to {:?} for shard {} and seqno {}",
                    name, single, &from, &to, shard_id, seqno
                );
                metrics.state.force_apply_hostname.inc();
            }
            *single = to
        }
        Insert(_) => return Err(format!("cannot insert {} field", name)),
        Delete(_) => return Err(format!("cannot delete {} field", name)),
    }
    Ok(())
}

fn diff_field_sorted_iter<'a, K, V, IF, IT>(from: IF, to: IT, diffs: &mut Vec<StateFieldDiff<K, V>>)
where
    K: Ord + Clone + 'a,
    V: PartialEq + Clone + 'a,
    IF: IntoIterator<Item = (&'a K, &'a V)>,
    IT: IntoIterator<Item = (&'a K, &'a V)>,
{
    let (mut from, mut to) = (from.into_iter(), to.into_iter());
    let (mut f, mut t) = (from.next(), to.next());
    loop {
        match (f, t) {
            (None, None) => break,
            (Some((fk, fv)), Some((tk, tv))) => match fk.cmp(tk) {
                Ordering::Less => {
                    diffs.push(StateFieldDiff {
                        key: fk.clone(),
                        val: Delete(fv.clone()),
                    });
                    let f_next = from.next();
                    debug_assert!(f_next.as_ref().map_or(true, |(fk_next, _)| fk_next > &fk));
                    f = f_next;
                }
                Ordering::Greater => {
                    diffs.push(StateFieldDiff {
                        key: tk.clone(),
                        val: Insert(tv.clone()),
                    });
                    let t_next = to.next();
                    debug_assert!(t_next.as_ref().map_or(true, |(tk_next, _)| tk_next > &tk));
                    t = t_next;
                }
                Ordering::Equal => {
                    // TODO: regression test for this if, I missed it in the
                    // original impl :)
                    if fv != tv {
                        diffs.push(StateFieldDiff {
                            key: fk.clone(),
                            val: Update(fv.clone(), tv.clone()),
                        });
                    }
                    let f_next = from.next();
                    debug_assert!(f_next.as_ref().map_or(true, |(fk_next, _)| fk_next > &fk));
                    f = f_next;
                    let t_next = to.next();
                    debug_assert!(t_next.as_ref().map_or(true, |(tk_next, _)| tk_next > &tk));
                    t = t_next;
                }
            },
            (None, Some((tk, tv))) => {
                diffs.push(StateFieldDiff {
                    key: tk.clone(),
                    val: Insert(tv.clone()),
                });
                let t_next = to.next();
                debug_assert!(t_next.as_ref().map_or(true, |(tk_next, _)| tk_next > &tk));
                t = t_next;
            }
            (Some((fk, fv)), None) => {
                diffs.push(StateFieldDiff {
                    key: fk.clone(),
                    val: Delete(fv.clone()),
                });
                let f_next = from.next();
                debug_assert!(f_next.as_ref().map_or(true, |(fk_next, _)| fk_next > &fk));
                f = f_next;
            }
        }
    }
}

fn apply_diffs_map<K: Ord, V: PartialEq + Debug>(
    name: &str,
    diffs: impl IntoIterator<Item = StateFieldDiff<K, V>>,
    map: &mut BTreeMap<K, V>,
) -> Result<(), String> {
    for diff in diffs {
        apply_diff_map(name, diff, map)?;
    }
    Ok(())
}

// This might leave state in an invalid (umm) state when returning an error. The
// caller ultimately ends up panic'ing on error, but if that changes, we might
// want to revisit this.
fn apply_diff_map<K: Ord, V: PartialEq + Debug>(
    name: &str,
    diff: StateFieldDiff<K, V>,
    map: &mut BTreeMap<K, V>,
) -> Result<(), String> {
    match diff.val {
        Insert(to) => {
            let prev = map.insert(diff.key, to);
            if prev != None {
                return Err(format!("{} insert found existing value: {:?}", name, prev));
            }
        }
        Update(from, to) => {
            let prev = map.insert(diff.key, to);
            if prev.as_ref() != Some(&from) {
                return Err(format!(
                    "{} update didn't match: {:?} vs {:?}",
                    name,
                    prev,
                    Some(from),
                ));
            }
        }
        Delete(from) => {
            let prev = map.remove(&diff.key);
            if prev.as_ref() != Some(&from) {
                return Err(format!(
                    "{} delete didn't match: {:?} vs {:?}",
                    name,
                    prev,
                    Some(from),
                ));
            }
        }
    };
    Ok(())
}

// This might leave state in an invalid (umm) state when returning an error. The
// caller ultimately ends up panic'ing on error, but if that changes, we might
// want to revisit this.
fn apply_diffs_spine<T: Timestamp + Lattice + Codec64>(
    metrics: &Metrics,
    mut diffs: Vec<StateFieldDiff<HollowBatch<T>, ()>>,
    trace: &mut Trace<T>,
) -> Result<(), String> {
    // Another special case: sniff out a newly inserted batch (one whose lower
    // lines up with the current upper) and handle that now. Then fall through
    // to the rest of the handling on whatever is left.
    if let Some(insert) = sniff_insert(&mut diffs, trace.upper()) {
        // Ignore merge_reqs because whichever process generated this diff is
        // assigned the work.
        let () = trace.push_batch_no_merge_reqs(insert);
        // If this insert was the only thing in diffs, then return now instead
        // of falling through to the "no diffs" case in the match so we can inc
        // the apply_spine_fast_path metric.
        if diffs.is_empty() {
            metrics.state.apply_spine_fast_path.inc();
            return Ok(());
        }
    }

    match &diffs[..] {
        // Fast-path: no diffs.
        [] => return Ok(()),

        // Fast-path: batch insert with both new and most recent batch empty.
        // Spine will happily merge these empty batches together without a call
        // out to compaction.
        [
            StateFieldDiff {
                key: del,
                val: StateFieldValDiff::Delete(()),
            },
            StateFieldDiff {
                key: ins,
                val: StateFieldValDiff::Insert(()),
            },
        ] => {
            if del.is_empty()
                && ins.is_empty()
                && del.desc.lower() == ins.desc.lower()
                && PartialOrder::less_than(del.desc.upper(), ins.desc.upper())
            {
                // Ignore merge_reqs because whichever process generated this diff is
                // assigned the work.
                let () = trace.push_batch_no_merge_reqs(HollowBatch::empty(Description::new(
                    del.desc.upper().clone(),
                    ins.desc.upper().clone(),
                    // `keys.len() == 0` for both `del` and `ins` means we
                    // don't have to think about what the compaction
                    // frontier is for these batches (nothing in them, so nothing could have been compacted.
                    Antichain::from_elem(T::minimum()),
                )));
                metrics.state.apply_spine_fast_path.inc();
                return Ok(());
            }
        }
        // Fall-through
        _ => {}
    }

    // Fast-path: compaction
    if let Some((_inputs, output)) = sniff_compaction(&diffs) {
        let res = FueledMergeRes {
            output,
            input: CompactionInput::Legacy,
            new_active_compaction: None,
        };
        // We can't predict how spine will arrange the batches when it's
        // hydrated. This means that something that is maintaining a Spine
        // starting at some seqno may not exactly match something else
        // maintaining the same spine starting at a different seqno. (Plus,
        // maybe these aren't even on the same version of the code and we've
        // changed the spine logic.) Because apply_merge_res is strict,
        // we're not _guaranteed_ that we can apply a compaction response
        // that was generated elsewhere. Most of the time we can, though, so
        // count the good ones and fall back to the slow path below when we
        // can't.
        if trace.apply_merge_res_unchecked(&res).applied() {
            // Maybe return the replaced batches from apply_merge_res and verify
            // that they match _inputs?
            metrics.state.apply_spine_fast_path.inc();
            return Ok(());
        }

        // Otherwise, try our lenient application of a compaction result.
        let mut batches = Vec::new();
        trace.map_batches(|b| batches.push(b.clone()));

        match apply_compaction_lenient(metrics, batches, &res.output) {
            Ok(batches) => {
                let mut new_trace = Trace::default();
                new_trace.roundtrip_structure = trace.roundtrip_structure;
                new_trace.downgrade_since(trace.since());
                for batch in batches {
                    // Ignore merge_reqs because whichever process generated
                    // this diff is assigned the work.
                    let () = new_trace.push_batch_no_merge_reqs(batch.clone());
                }
                *trace = new_trace;
                metrics.state.apply_spine_slow_path_lenient.inc();
                return Ok(());
            }
            Err(err) => {
                return Err(format!(
                    "lenient compaction result apply unexpectedly failed: {}",
                    err
                ));
            }
        }
    }

    // Something complicated is going on, so reconstruct the Trace from scratch.
    metrics.state.apply_spine_slow_path.inc();
    debug!(
        "apply_diffs_spine didn't hit a fast-path diffs={:?} trace={:?}",
        diffs, trace
    );

    let batches = {
        let mut batches = BTreeMap::new();
        trace.map_batches(|b| assert_none!(batches.insert(b.clone(), ())));
        apply_diffs_map("spine", diffs.clone(), &mut batches).map(|_ok| batches)
    };

    let batches = match batches {
        Ok(batches) => batches,
        Err(err) => {
            metrics
                .state
                .apply_spine_slow_path_with_reconstruction
                .inc();
            debug!(
                "apply_diffs_spines could not apply diffs directly to existing trace batches: {}. diffs={:?} trace={:?}",
                err, diffs, trace
            );
            // if we couldn't apply our diffs directly to our trace's batches, we can
            // try one more trick: reconstruct a new spine with our existing batches,
            // in an attempt to create different merges than we currently have. then,
            // we can try to apply our diffs on top of these new (potentially) merged
            // batches.
            let mut reconstructed_spine = Trace::default();
            reconstructed_spine.roundtrip_structure = trace.roundtrip_structure;
            trace.map_batches(|b| {
                // Ignore merge_reqs because whichever process generated this
                // diff is assigned the work.
                let () = reconstructed_spine.push_batch_no_merge_reqs(b.clone());
            });

            let mut batches = BTreeMap::new();
            reconstructed_spine.map_batches(|b| assert_none!(batches.insert(b.clone(), ())));
            apply_diffs_map("spine", diffs, &mut batches)?;
            batches
        }
    };

    let mut new_trace = Trace::default();
    new_trace.roundtrip_structure = trace.roundtrip_structure;
    new_trace.downgrade_since(trace.since());
    for (batch, ()) in batches {
        // Ignore merge_reqs because whichever process generated this diff is
        // assigned the work.
        let () = new_trace.push_batch_no_merge_reqs(batch);
    }
    *trace = new_trace;
    Ok(())
}

fn sniff_insert<T: Timestamp + Lattice>(
    diffs: &mut Vec<StateFieldDiff<HollowBatch<T>, ()>>,
    upper: &Antichain<T>,
) -> Option<HollowBatch<T>> {
    for idx in 0..diffs.len() {
        match &diffs[idx] {
            StateFieldDiff {
                key,
                val: StateFieldValDiff::Insert(()),
            } if key.desc.lower() == upper => return Some(diffs.remove(idx).key),
            _ => continue,
        }
    }
    None
}

// TODO: Instead of trying to sniff out a compaction from diffs, should we just
// be explicit?
fn sniff_compaction<'a, T: Timestamp + Lattice>(
    diffs: &'a [StateFieldDiff<HollowBatch<T>, ()>],
) -> Option<(Vec<&'a HollowBatch<T>>, HollowBatch<T>)> {
    // Compaction always produces exactly one output batch (with possibly many
    // parts, but we get one Insert for the whole batch.
    let mut inserts = diffs.iter().flat_map(|x| match x.val {
        StateFieldValDiff::Insert(()) => Some(&x.key),
        _ => None,
    });
    let compaction_output = match inserts.next() {
        Some(x) => x,
        None => return None,
    };
    if let Some(_) = inserts.next() {
        return None;
    }

    // Grab all deletes and sanity check that there are no updates.
    let mut compaction_inputs = Vec::with_capacity(diffs.len() - 1);
    for diff in diffs.iter() {
        match diff.val {
            StateFieldValDiff::Delete(()) => {
                compaction_inputs.push(&diff.key);
            }
            StateFieldValDiff::Insert(()) => {}
            StateFieldValDiff::Update((), ()) => {
                // Fall through to let the general case create the error
                // message.
                return None;
            }
        }
    }

    Some((compaction_inputs, compaction_output.clone()))
}

/// Apply a compaction diff that doesn't exactly line up with the set of
/// HollowBatches.
///
/// Because of the way Spine internally optimizes only _some_ empty batches
/// (immediately merges them in), we can end up in a situation where a
/// compaction res applied on another copy of state, but when we replay all of
/// the state diffs against a new Spine locally, it merges empty batches
/// differently in-mem and we can't exactly apply the compaction diff. Example:
///
/// - compact: [1,2),[2,3) -> [1,3)
/// - this spine: [0,2),[2,3) (0,1 is empty)
///
/// Ideally, we'd figure out a way to avoid this, but nothing immediately comes
/// to mind. In the meantime, force the application (otherwise the shard is
/// stuck and we can't do anything with it) by manually splitting the empty
/// batch back out. For the example above:
///
/// - [0,1),[1,3) (0,1 is empty)
///
/// This can only happen when the batch needing to be split is empty, so error
/// out if it isn't because that means something unexpected is going on.
fn apply_compaction_lenient<'a, T: Timestamp + Lattice>(
    metrics: &Metrics,
    mut trace: Vec<HollowBatch<T>>,
    replacement: &'a HollowBatch<T>,
) -> Result<Vec<HollowBatch<T>>, String> {
    let mut overlapping_batches = Vec::new();
    trace.retain(|b| {
        let before_replacement = PartialOrder::less_equal(b.desc.upper(), replacement.desc.lower());
        let after_replacement = PartialOrder::less_equal(replacement.desc.upper(), b.desc.lower());
        let overlaps_replacement = !(before_replacement || after_replacement);
        if overlaps_replacement {
            overlapping_batches.push(b.clone());
            false
        } else {
            true
        }
    });

    {
        let first_overlapping_batch = match overlapping_batches.first() {
            Some(x) => x,
            None => return Err("replacement didn't overlap any batches".into()),
        };
        if PartialOrder::less_than(
            first_overlapping_batch.desc.lower(),
            replacement.desc.lower(),
        ) {
            if first_overlapping_batch.len > 0 {
                return Err(format!(
                    "overlapping batch was unexpectedly non-empty: {:?}",
                    first_overlapping_batch
                ));
            }
            let desc = Description::new(
                first_overlapping_batch.desc.lower().clone(),
                replacement.desc.lower().clone(),
                first_overlapping_batch.desc.since().clone(),
            );
            trace.push(HollowBatch::empty(desc));
            metrics.state.apply_spine_slow_path_lenient_adjustment.inc();
        }
    }

    {
        let last_overlapping_batch = match overlapping_batches.last() {
            Some(x) => x,
            None => return Err("replacement didn't overlap any batches".into()),
        };
        if PartialOrder::less_than(
            replacement.desc.upper(),
            last_overlapping_batch.desc.upper(),
        ) {
            if last_overlapping_batch.len > 0 {
                return Err(format!(
                    "overlapping batch was unexpectedly non-empty: {:?}",
                    last_overlapping_batch
                ));
            }
            let desc = Description::new(
                replacement.desc.upper().clone(),
                last_overlapping_batch.desc.upper().clone(),
                last_overlapping_batch.desc.since().clone(),
            );
            trace.push(HollowBatch::empty(desc));
            metrics.state.apply_spine_slow_path_lenient_adjustment.inc();
        }
    }
    trace.push(replacement.clone());

    // We just inserted stuff at the end, so re-sort them into place.
    trace.sort_by(|a, b| a.desc.lower().elements().cmp(b.desc.lower().elements()));

    // This impl is a touch complex, so sanity check our work.
    let mut expected_lower = &Antichain::from_elem(T::minimum());
    for b in trace.iter() {
        if b.desc.lower() != expected_lower {
            return Err(format!(
                "lower {:?} did not match expected {:?}: {:?}",
                b.desc.lower(),
                expected_lower,
                trace
            ));
        }
        expected_lower = b.desc.upper();
    }
    Ok(trace)
}

/// A type that facilitates the proto encoding of a [`ProtoStateFieldDiffs`]
///
/// [`ProtoStateFieldDiffs`] is a columnar encoding of [`StateFieldDiff`]s, see
/// its doc comment for more info. The underlying buffer for a [`ProtoStateFieldDiffs`]
/// is a [`Bytes`] struct, which is an immutable, shared, reference counted,
/// buffer of data. Using a [`Bytes`] struct is a very efficient way to manage data
/// becuase multiple [`Bytes`] can reference different parts of the same underlying
/// portion of memory. See its doc comment for more info.
///
/// A [`ProtoStateFieldDiffsWriter`] maintains a mutable, unique, data buffer, i.e.
/// a [`BytesMut`], which we use when encoding a [`StateFieldDiff`]. And when
/// finished encoding, we convert it into a [`ProtoStateFieldDiffs`] by "freezing" the
/// underlying buffer, converting it into a [`Bytes`] struct, so it can be shared.
///
/// [`Bytes`]: bytes::Bytes
#[derive(Debug)]
pub struct ProtoStateFieldDiffsWriter {
    data_buf: BytesMut,
    proto: ProtoStateFieldDiffs,
}

impl ProtoStateFieldDiffsWriter {
    /// Record a [`ProtoStateField`] for our columnar encoding.
    pub fn push_field(&mut self, field: ProtoStateField) {
        self.proto.fields.push(i32::from(field));
    }

    /// Record a [`ProtoStateFieldDiffType`] for our columnar encoding.
    pub fn push_diff_type(&mut self, diff_type: ProtoStateFieldDiffType) {
        self.proto.diff_types.push(i32::from(diff_type));
    }

    /// Encode a message for our columnar encoding.
    pub fn encode_proto<M: prost::Message>(&mut self, msg: &M) {
        let len_before = self.data_buf.len();
        self.data_buf.reserve(msg.encoded_len());

        // Note: we use `encode_raw` as opposed to `encode` because all `encode` does is
        // check to make sure there's enough bytes in the buffer to fit our message
        // which we know there are because we just reserved the space. When benchmarking
        // `encode_raw` does offer a slight performance improvement over `encode`.
        msg.encode_raw(&mut self.data_buf);

        // Record exactly how many bytes were written.
        let written_len = self.data_buf.len() - len_before;
        self.proto.data_lens.push(u64::cast_from(written_len));
    }

    pub fn into_proto(self) -> ProtoStateFieldDiffs {
        let ProtoStateFieldDiffsWriter {
            data_buf,
            mut proto,
        } = self;

        // Assert we didn't write into the proto's data_bytes field
        assert!(proto.data_bytes.is_empty());

        // Move our buffer into the proto
        let data_bytes = data_buf.freeze();
        proto.data_bytes = data_bytes;

        proto
    }
}

impl ProtoStateFieldDiffs {
    pub fn into_writer(mut self) -> ProtoStateFieldDiffsWriter {
        // Create a new buffer which we'll encode data into.
        let mut data_buf = BytesMut::with_capacity(self.data_bytes.len());

        // Take our existing data, and copy it into our buffer.
        let existing_data = std::mem::take(&mut self.data_bytes);
        data_buf.extend(existing_data);

        ProtoStateFieldDiffsWriter {
            data_buf,
            proto: self,
        }
    }

    pub fn iter<'a>(&'a self) -> ProtoStateFieldDiffsIter<'a> {
        let len = self.fields.len();
        assert_eq!(self.diff_types.len(), len);

        ProtoStateFieldDiffsIter {
            len,
            diff_idx: 0,
            data_idx: 0,
            data_offset: 0,
            diffs: self,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.fields.len() != self.diff_types.len() {
            return Err(format!(
                "fields {} and diff_types {} lengths disagree",
                self.fields.len(),
                self.diff_types.len()
            ));
        }

        let mut expected_data_slices = 0;
        for diff_type in self.diff_types.iter() {
            // We expect one for the key.
            expected_data_slices += 1;
            // And 1 or 2 for val depending on the diff type.
            match ProtoStateFieldDiffType::try_from(*diff_type) {
                Ok(ProtoStateFieldDiffType::Insert) => expected_data_slices += 1,
                Ok(ProtoStateFieldDiffType::Update) => expected_data_slices += 2,
                Ok(ProtoStateFieldDiffType::Delete) => expected_data_slices += 1,
                Err(_) => return Err(format!("unknown diff_type {}", diff_type)),
            }
        }
        if expected_data_slices != self.data_lens.len() {
            return Err(format!(
                "expected {} data slices got {}",
                expected_data_slices,
                self.data_lens.len()
            ));
        }

        let expected_data_bytes = usize::cast_from(self.data_lens.iter().copied().sum::<u64>());
        if expected_data_bytes != self.data_bytes.len() {
            return Err(format!(
                "expected {} data bytes got {}",
                expected_data_bytes,
                self.data_bytes.len()
            ));
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ProtoStateFieldDiff<'a> {
    pub key: &'a [u8],
    pub diff_type: ProtoStateFieldDiffType,
    pub from: &'a [u8],
    pub to: &'a [u8],
}

pub struct ProtoStateFieldDiffsIter<'a> {
    len: usize,
    diff_idx: usize,
    data_idx: usize,
    data_offset: usize,
    diffs: &'a ProtoStateFieldDiffs,
}

impl<'a> Iterator for ProtoStateFieldDiffsIter<'a> {
    type Item = Result<(ProtoStateField, ProtoStateFieldDiff<'a>), TryFromProtoError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.diff_idx >= self.len {
            return None;
        }
        let mut next_data = || {
            let start = self.data_offset;
            let end = start + usize::cast_from(self.diffs.data_lens[self.data_idx]);
            let data = &self.diffs.data_bytes[start..end];
            self.data_idx += 1;
            self.data_offset = end;
            data
        };
        let field = match ProtoStateField::try_from(self.diffs.fields[self.diff_idx]) {
            Ok(x) => x,
            Err(_) => {
                return Some(Err(TryFromProtoError::unknown_enum_variant(format!(
                    "ProtoStateField({})",
                    self.diffs.fields[self.diff_idx]
                ))));
            }
        };
        let diff_type =
            match ProtoStateFieldDiffType::try_from(self.diffs.diff_types[self.diff_idx]) {
                Ok(x) => x,
                Err(_) => {
                    return Some(Err(TryFromProtoError::unknown_enum_variant(format!(
                        "ProtoStateFieldDiffType({})",
                        self.diffs.diff_types[self.diff_idx]
                    ))));
                }
            };
        let key = next_data();
        let (from, to): (&[u8], &[u8]) = match diff_type {
            ProtoStateFieldDiffType::Insert => (&[], next_data()),
            ProtoStateFieldDiffType::Update => (next_data(), next_data()),
            ProtoStateFieldDiffType::Delete => (next_data(), &[]),
        };
        let diff = ProtoStateFieldDiff {
            key,
            diff_type,
            from,
            to,
        };
        self.diff_idx += 1;
        Some(Ok((field, diff)))
    }
}

#[cfg(test)]
mod tests {
    use semver::Version;
    use std::ops::ControlFlow::Continue;

    use crate::internal::paths::{PartId, PartialBatchKey, RollupId, WriterKey};
    use mz_ore::metrics::MetricsRegistry;

    use crate::ShardId;
    use crate::internal::state::TypedState;

    use super::*;

    /// Model a situation where a "leader" is constantly making changes to its state, and a "follower"
    /// is applying those changes as diffs.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn test_state_sync() {
        use proptest::prelude::*;

        #[derive(Debug, Clone)]
        enum Action {
            /// Append a (non)empty batch to the shard that covers the given length of time.
            Append { empty: bool, time_delta: u64 },
            /// Apply the Nth compaction request we've received to the shard state.
            Compact { req: usize },
        }

        let action_gen: BoxedStrategy<Action> = {
            prop::strategy::Union::new([
                (any::<bool>(), 1u64..10u64)
                    .prop_map(|(empty, time_delta)| Action::Append { empty, time_delta })
                    .boxed(),
                (0usize..10usize)
                    .prop_map(|req| Action::Compact { req })
                    .boxed(),
            ])
            .boxed()
        };

        fn run(actions: Vec<(Action, bool)>, metrics: &Metrics) {
            let version = Version::new(0, 100, 0);
            let writer_key = WriterKey::Version(version.to_string());
            let id = ShardId::new();
            let hostname = "computer";
            let typed: TypedState<String, (), u64, i64> =
                TypedState::new(version, id, hostname.to_string(), 0);
            let mut leader = typed.state;

            let seqno = SeqNo::minimum();
            let mut lower = 0u64;
            let mut merge_reqs = vec![];

            leader.collections.rollups.insert(
                seqno,
                HollowRollup {
                    key: PartialRollupKey::new(seqno, &RollupId::new()),
                    encoded_size_bytes: None,
                },
            );
            leader.collections.trace.roundtrip_structure = false;
            let mut follower = leader.clone();

            for (action, roundtrip_structure) in actions {
                // Apply the given action and the new roundtrip_structure setting and take a diff.
                let mut old_leader = leader.clone();
                match action {
                    Action::Append { empty, time_delta } => {
                        let upper = lower + time_delta;
                        let key = if empty {
                            None
                        } else {
                            let id = PartId::new();
                            Some(PartialBatchKey::new(&writer_key, &id))
                        };

                        let keys = key.as_ref().map(|k| k.0.as_str());
                        let reqs = leader.collections.trace.push_batch(
                            crate::internal::state::tests::hollow(
                                lower,
                                upper,
                                keys.as_slice(),
                                if empty { 0 } else { 1 },
                            ),
                        );
                        merge_reqs.extend(reqs);
                        lower = upper;
                    }
                    Action::Compact { req } => {
                        if !merge_reqs.is_empty() {
                            let req = merge_reqs.remove(req.min(merge_reqs.len() - 1));
                            let len = req.inputs.iter().map(|p| p.batch.len).sum();
                            let parts = req
                                .inputs
                                .into_iter()
                                .flat_map(|p| p.batch.parts.clone())
                                .collect();
                            let output = HollowBatch::new_run(req.desc, parts, len);
                            leader
                                .collections
                                .trace
                                .apply_merge_res_unchecked(&FueledMergeRes {
                                    output,
                                    input: CompactionInput::Legacy,
                                    new_active_compaction: None,
                                });
                        }
                    }
                }
                leader.collections.trace.roundtrip_structure = roundtrip_structure;
                leader.seqno.0 += 1;
                let diff = StateDiff::from_diff(&old_leader, &leader);

                // Validate that the diff applies to both the previous state (also checked in
                // debug asserts) and our follower that's only synchronized via diffs.
                old_leader
                    .apply_diff(metrics, diff.clone())
                    .expect("diff applies to the old version of the leader state");
                follower
                    .apply_diff(metrics, diff.clone())
                    .expect("diff applies to the synced version of the follower state");

                // TODO: once spine structure is roundtripped through diffs, assert that the follower
                // has the same batches etc. as the leader does.
            }
        }

        let config = PersistConfig::new_for_tests();
        let metrics_registry = MetricsRegistry::new();
        let metrics: Metrics = Metrics::new(&config, &metrics_registry);

        proptest!(|(actions in prop::collection::vec((action_gen, any::<bool>()), 1..20))| {
            run(actions, &metrics)
        })
    }

    // Regression test for the apply_diffs_spine special case that sniffs out an
    // insert, applies it, and then lets the remaining diffs (if any) fall
    // through to the rest of the code. See database-issues#4431.
    #[mz_ore::test]
    fn regression_15493_sniff_insert() {
        fn hb(lower: u64, upper: u64, len: usize) -> HollowBatch<u64> {
            HollowBatch::new_run(
                Description::new(
                    Antichain::from_elem(lower),
                    Antichain::from_elem(upper),
                    Antichain::from_elem(0),
                ),
                Vec::new(),
                len,
            )
        }

        // The bug handled here is essentially a set of batches that look like
        // the pattern matched by `apply_lenient` _plus_ an insert. In
        // apply_diffs_spine, we use `sniff_insert` to steal the insert out of
        // the diffs and fall back to the rest of the logic to handle the
        // remaining diffs.
        //
        // Concretely, something like (the numbers are truncated versions of the
        // actual bug posted in the issue):
        // - spine: [0][7094664]0, [7094664][7185234]100
        // - diffs: [0][6805359]0 del, [6805359][7083793]0 del, [0][7083793]0 ins,
        //   [7185234][7185859]20 ins
        //
        // Where this allows us to handle the [7185234,7185859) and then
        // apply_lenient handles splitting up [0,7094664) so we can apply the
        // [0,6805359)+[6805359,7083793)->[0,7083793) swap.

        let batches_before = vec![hb(0, 7094664, 0), hb(7094664, 7185234, 100)];

        let diffs = vec![
            StateFieldDiff {
                key: hb(0, 6805359, 0),
                val: StateFieldValDiff::Delete(()),
            },
            StateFieldDiff {
                key: hb(6805359, 7083793, 0),
                val: StateFieldValDiff::Delete(()),
            },
            StateFieldDiff {
                key: hb(0, 7083793, 0),
                val: StateFieldValDiff::Insert(()),
            },
            StateFieldDiff {
                key: hb(7185234, 7185859, 20),
                val: StateFieldValDiff::Insert(()),
            },
        ];

        // Ideally this first batch would be [0][7083793], [7083793,7094664]
        // here because `apply_lenient` splits it out, but when `apply_lenient`
        // reconstructs the trace, Spine happens to (deterministically) collapse
        // them back together. The main value of this test is that the
        // `apply_diffs_spine` call below doesn't return an Err, so don't worry
        // too much about this, it's just a sanity check.
        let batches_after = vec![
            hb(0, 7094664, 0),
            hb(7094664, 7185234, 100),
            hb(7185234, 7185859, 20),
        ];

        let cfg = PersistConfig::new_for_tests();
        let state = TypedState::<(), (), u64, i64>::new(
            cfg.build_version.clone(),
            ShardId::new(),
            cfg.hostname.clone(),
            (cfg.now)(),
        );
        let state = state.clone_apply(&cfg, &mut |_seqno, _cfg, state| {
            for b in batches_before.iter() {
                let _merge_reqs = state.trace.push_batch(b.clone());
            }
            Continue::<(), ()>(())
        });
        let mut state = match state {
            Continue((_, x)) => x,
            _ => unreachable!(),
        };

        let metrics = Metrics::new(&PersistConfig::new_for_tests(), &MetricsRegistry::new());
        assert_eq!(
            apply_diffs_spine(&metrics, diffs, &mut state.collections.trace),
            Ok(())
        );

        let mut actual = Vec::new();
        state
            .collections
            .trace
            .map_batches(|b| actual.push(b.clone()));
        assert_eq!(actual, batches_after);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn apply_lenient() {
        #[track_caller]
        fn testcase(
            replacement: (u64, u64, u64, usize),
            spine: &[(u64, u64, u64, usize)],
            expected: Result<&[(u64, u64, u64, usize)], &str>,
        ) {
            fn batch(x: &(u64, u64, u64, usize)) -> HollowBatch<u64> {
                let (lower, upper, since, len) = x;
                let desc = Description::new(
                    Antichain::from_elem(*lower),
                    Antichain::from_elem(*upper),
                    Antichain::from_elem(*since),
                );
                HollowBatch::new_run(desc, Vec::new(), *len)
            }
            let replacement = batch(&replacement);
            let batches = spine.iter().map(batch).collect::<Vec<_>>();

            let metrics = Metrics::new(&PersistConfig::new_for_tests(), &MetricsRegistry::new());
            let actual = apply_compaction_lenient(&metrics, batches, &replacement);
            let expected = match expected {
                Ok(batches) => Ok(batches.iter().map(batch).collect::<Vec<_>>()),
                Err(err) => Err(err.to_owned()),
            };
            assert_eq!(actual, expected);
        }

        // Exact swap of N batches
        testcase(
            (0, 3, 0, 100),
            &[(0, 1, 0, 0), (1, 2, 0, 0), (2, 3, 0, 0)],
            Ok(&[(0, 3, 0, 100)]),
        );

        // Swap out the middle of a batch
        testcase(
            (1, 2, 0, 100),
            &[(0, 3, 0, 0)],
            Ok(&[(0, 1, 0, 0), (1, 2, 0, 100), (2, 3, 0, 0)]),
        );

        // Split batch at replacement lower
        testcase(
            (2, 4, 0, 100),
            &[(0, 3, 0, 0), (3, 4, 0, 0)],
            Ok(&[(0, 2, 0, 0), (2, 4, 0, 100)]),
        );

        // Err: split batch at replacement lower not empty
        testcase(
            (2, 4, 0, 100),
            &[(0, 3, 0, 1), (3, 4, 0, 0)],
            Err(
                "overlapping batch was unexpectedly non-empty: HollowBatch { desc: ([0], [3], [0]), parts: [], len: 1, runs: [], run_meta: [] }",
            ),
        );

        // Split batch at replacement lower (untouched batch before the split one)
        testcase(
            (2, 4, 0, 100),
            &[(0, 1, 0, 0), (1, 3, 0, 0), (3, 4, 0, 0)],
            Ok(&[(0, 1, 0, 0), (1, 2, 0, 0), (2, 4, 0, 100)]),
        );

        // Split batch at replacement lower (since is preserved)
        testcase(
            (2, 4, 0, 100),
            &[(0, 3, 200, 0), (3, 4, 0, 0)],
            Ok(&[(0, 2, 200, 0), (2, 4, 0, 100)]),
        );

        // Split batch at replacement upper
        testcase(
            (0, 2, 0, 100),
            &[(0, 1, 0, 0), (1, 4, 0, 0)],
            Ok(&[(0, 2, 0, 100), (2, 4, 0, 0)]),
        );

        // Err: split batch at replacement upper not empty
        testcase(
            (0, 2, 0, 100),
            &[(0, 1, 0, 0), (1, 4, 0, 1)],
            Err(
                "overlapping batch was unexpectedly non-empty: HollowBatch { desc: ([1], [4], [0]), parts: [], len: 1, runs: [], run_meta: [] }",
            ),
        );

        // Split batch at replacement upper (untouched batch after the split one)
        testcase(
            (0, 2, 0, 100),
            &[(0, 1, 0, 0), (1, 3, 0, 0), (3, 4, 0, 0)],
            Ok(&[(0, 2, 0, 100), (2, 3, 0, 0), (3, 4, 0, 0)]),
        );

        // Split batch at replacement upper (since is preserved)
        testcase(
            (0, 2, 0, 100),
            &[(0, 1, 0, 0), (1, 4, 200, 0)],
            Ok(&[(0, 2, 0, 100), (2, 4, 200, 0)]),
        );

        // Split batch at replacement lower and upper
        testcase(
            (2, 6, 0, 100),
            &[(0, 3, 0, 0), (3, 5, 0, 0), (5, 8, 0, 0)],
            Ok(&[(0, 2, 0, 0), (2, 6, 0, 100), (6, 8, 0, 0)]),
        );

        // Replacement doesn't overlap (after)
        testcase(
            (2, 3, 0, 100),
            &[(0, 1, 0, 0)],
            Err("replacement didn't overlap any batches"),
        );

        // Replacement doesn't overlap (before, though this would never happen in practice)
        testcase(
            (2, 3, 0, 100),
            &[(4, 5, 0, 0)],
            Err("replacement didn't overlap any batches"),
        );
    }
}
