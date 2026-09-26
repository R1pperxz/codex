use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::ThreadHistoryMode;
use std::path::Path;
use std::sync::Arc;

use super::LocalThreadStore;
use super::live_writer;
use super::model_context;
use super::rollout_lineage::RolloutLineage;
use super::rollout_lineage::RolloutLineageSegment;
use super::thread_history::find_source_turn;
use super::thread_history::find_visible_turn;
use crate::ForkBoundary;
use crate::PrepareForkParams;
use crate::PreparedFork;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

pub(super) async fn prepare(
    store: &LocalThreadStore,
    params: PrepareForkParams,
) -> ThreadStoreResult<PreparedFork> {
    let PrepareForkParams {
        thread_id,
        boundary,
        source_rollout_path,
    } = params;
    let source_reservation = store.live_writer_locks.reserve_lifecycle(thread_id).await;
    if let Some(source_rollout_path) = source_rollout_path {
        if !matches!(boundary, ForkBoundary::Latest) {
            return Err(ThreadStoreError::InvalidRequest {
                message: "explicit rollout recovery only supports the latest boundary".to_string(),
            });
        }
        let (lineage, history_base) =
            prepare_explicit_root(store, thread_id, source_rollout_path.as_path()).await?;
        let model_context = Arc::new(model_context::load_for_fork(lineage, Some(history_base)).await?);
        return Ok(PreparedFork::new(
            thread_id,
            Some(history_base),
            model_context,
            source_reservation,
        ));
    }
    // Keep the source reserved until persistence and lineage materialization finish, even if the
    // caller cancels fork preparation.
    let lineage_store = store.clone();
    let (lineage, source_reservation) = tokio::spawn(async move {
        match live_writer::persist_thread(&lineage_store, thread_id).await {
            Ok(()) | Err(ThreadStoreError::ThreadNotFound { .. }) => {}
            Err(err) => return Err(err),
        }
        let lineage = lineage_store
            .resolve_rollout_lineage_for_reference(thread_id)
            .await?;
        Ok::<_, ThreadStoreError>((lineage, source_reservation))
    })
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to resolve fork lineage: {err}"),
    })??;
    let source_segment = lineage
        .segments()
        .last()
        .ok_or_else(|| ThreadStoreError::Internal {
            message: "fork lineage has no source segment".to_string(),
        })?;
    if store.state_db.is_none() {
        return Err(ThreadStoreError::Unsupported {
            operation: "prepare_fork",
        });
    }
    if !matches!(boundary, ForkBoundary::Latest) {
        for segment in lineage
            .segments()
            .iter()
            .take(lineage.segments().len().saturating_sub(1))
        {
            let _ancestor_writer_guard = store.live_writer_locks.lock(segment.rollout_id()).await;
            super::thread_history_materialization::materialize_to_sqlite(
                store,
                segment.rollout_id(),
                segment.rollout_path.as_path(),
            )
            .await?;
        }
    }
    let source_writer_guard = store.live_writer_locks.lock(thread_id).await;
    super::thread_history_materialization::materialize_to_sqlite(
        store,
        source_segment.rollout_id(),
        source_segment.rollout_path.as_path(),
    )
    .await?;

    let history_base = history_base_at_boundary(store, thread_id, boundary, &lineage).await?;
    drop(source_writer_guard);
    let model_context = Arc::new(model_context::load_for_fork(lineage, history_base).await?);

    Ok(PreparedFork::new(
        thread_id,
        history_base,
        model_context,
        source_reservation,
    ))
}

async fn prepare_explicit_root(
    store: &LocalThreadStore,
    thread_id: codex_protocol::ThreadId,
    source_path: &Path,
) -> ThreadStoreResult<(RolloutLineage, HistoryPosition)> {
    let path = std::fs::canonicalize(source_path).map_err(|err| ThreadStoreError::InvalidRequest {
        message: format!("cannot resolve source rollout: {err}"),
    })?;
    let managed = [
        store.config.codex_home.join(codex_rollout::SESSIONS_SUBDIR),
        store.config.codex_home.join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR),
    ]
    .into_iter()
    .filter_map(|root| std::fs::canonicalize(root).ok())
    .any(|root| path.starts_with(root));
    if !managed || path.extension().is_none_or(|ext| ext != "jsonl") {
        return Err(ThreadStoreError::InvalidRequest {
            message: "source rollout must be a managed JSONL file".to_string(),
        });
    }
    let meta = codex_rollout::read_session_meta_line(path.as_path())
        .await
        .map_err(|err| ThreadStoreError::InvalidRequest {
            message: format!("cannot read source session metadata: {err}"),
        })?;
    if meta.meta.id != thread_id
        || meta.meta.history_mode != ThreadHistoryMode::Paginated
        || meta.meta.history_base.is_some()
    {
        return Err(ThreadStoreError::InvalidRequest {
            message: "source must be a paginated root belonging to the requested thread"
                .to_string(),
        });
    }
    let rollout_id = codex_rollout::rollout_id_from_path(path.as_path()).ok_or_else(|| {
        ThreadStoreError::InvalidRequest {
            message: "source rollout has an invalid file name".to_string(),
        }
    })?;
    let bytes = tokio::fs::read(path.as_path())
        .await
        .map_err(|err| ThreadStoreError::InvalidRequest {
            message: format!("cannot read source rollout: {err}"),
        })?;
    if !bytes.ends_with(b"\n") {
        return Err(ThreadStoreError::InvalidRequest {
            message: "source rollout must end at a complete JSONL record".to_string(),
        });
    }
    let mut next_ordinal = 0_u64;
    for line in bytes.split(|byte| *byte == b'\n').filter(|line| !line.is_empty()) {
        let parsed = codex_rollout::parse_rollout_line_bytes(line).map_err(|err| {
            ThreadStoreError::InvalidRequest {
                message: format!("source rollout contains an invalid record: {err}"),
            }
        })?;
        if parsed.ordinal != Some(next_ordinal) {
            return Err(ThreadStoreError::InvalidRequest {
                message: "source rollout ordinals are not contiguous".to_string(),
            });
        }
        next_ordinal += 1;
    }
    if next_ordinal == 0 {
        return Err(ThreadStoreError::InvalidRequest {
            message: "source rollout is empty".to_string(),
        });
    }
    let history_base = HistoryPosition {
        thread_id: rollout_id,
        end_ordinal_exclusive: next_ordinal,
        end_byte_offset: bytes.len() as u64,
    };
    let lineage = RolloutLineage {
        segments: vec![RolloutLineageSegment {
            rollout_id,
            rollout_path: path,
            start_ordinal: 1,
            end: None,
        }],
    };
    Ok((lineage, history_base))
}

pub(super) async fn history_base_at_boundary(
    store: &LocalThreadStore,
    thread_id: codex_protocol::ThreadId,
    boundary: ForkBoundary,
    lineage: &super::rollout_lineage::RolloutLineage,
) -> ThreadStoreResult<Option<HistoryPosition>> {
    let source_segment = lineage
        .segments()
        .last()
        .ok_or_else(|| ThreadStoreError::Internal {
            message: "fork lineage has no source segment".to_string(),
        })?;
    let latest_projection_state =
        super::thread_history::projection_state(store, source_segment.rollout_id())
            .await?
            .ok_or_else(|| ThreadStoreError::Internal {
                message: format!("missing projection state for paginated thread {thread_id}"),
            })?;
    let latest_position = HistoryPosition {
        thread_id: source_segment.rollout_id(),
        end_ordinal_exclusive: latest_projection_state.next_ordinal,
        end_byte_offset: latest_projection_state.next_byte_offset,
    };
    let pool = store.thread_history_db().await?;
    let position = match boundary {
        ForkBoundary::Latest => latest_position,
        ForkBoundary::ThroughTurn(turn_id) => {
            let row = find_visible_turn(pool, lineage, turn_id.as_str()).await?;
            if row.status == "inProgress" {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("lastTurnId '{turn_id}' identifies an in-progress turn"),
                });
            }
            let rollout_end_ordinal = row
                .rollout_end_ordinal
                .ok_or_else(|| missing_turn_position(turn_id.as_str()))?;
            let rollout_end_byte_offset = row
                .rollout_end_byte_offset
                .ok_or_else(|| missing_turn_position(turn_id.as_str()))?;
            HistoryPosition {
                thread_id: row.rollout_id,
                end_ordinal_exclusive: u64::try_from(rollout_end_ordinal)
                    .map_err(|_| invalid_turn_position(turn_id.as_str()))?
                    .checked_add(1)
                    .ok_or_else(|| invalid_turn_position(turn_id.as_str()))?,
                end_byte_offset: u64::try_from(rollout_end_byte_offset)
                    .map_err(|_| invalid_turn_position(turn_id.as_str()))?,
            }
        }
        ForkBoundary::BeforeTurn(turn_id) => {
            let row = find_source_turn(pool, lineage, turn_id.as_str()).await?;
            if row.rollout_end_ordinal == Some(row.rollout_ordinal) {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("turn {turn_id} does not have a persisted start boundary"),
                });
            }
            let rollout_byte_offset = row
                .rollout_byte_offset
                .ok_or_else(|| missing_turn_position(turn_id.as_str()))?;
            HistoryPosition {
                thread_id: row.rollout_id,
                end_ordinal_exclusive: u64::try_from(row.rollout_ordinal)
                    .map_err(|_| invalid_turn_position(turn_id.as_str()))?,
                end_byte_offset: u64::try_from(rollout_byte_offset)
                    .map_err(|_| invalid_turn_position(turn_id.as_str()))?,
            }
        }
    };
    let segment_index = lineage
        .segments()
        .iter()
        .position(|segment| segment.rollout_id() == position.thread_id)
        .ok_or_else(|| ThreadStoreError::Internal {
            message: "fork position is outside the source lineage".to_string(),
        })?;
    if lineage.segments()[segment_index].end.is_some_and(|end| {
        position.end_ordinal_exclusive > end.end_ordinal_exclusive
            || position.end_byte_offset > end.end_byte_offset
    }) {
        return Err(ThreadStoreError::InvalidRequest {
            message: "fork boundary exceeds inherited source history".to_string(),
        });
    }
    let history_base =
        if position.end_ordinal_exclusive == lineage.segments()[segment_index].start_ordinal() {
            segment_index
                .checked_sub(1)
                .and_then(|index| lineage.segments()[index].end)
        } else {
            Some(position)
        };
    Ok(history_base)
}

fn missing_turn_position(turn_id: &str) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: format!("turn {turn_id} does not have persisted rollout positions"),
    }
}

fn invalid_turn_position(turn_id: &str) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("invalid rollout position for turn {turn_id}"),
    }
}
