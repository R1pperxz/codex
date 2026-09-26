//! Classifies physical rollout segments owned by an indexed thread's paginated history.

use super::RolloutAuditFile;
use super::has_matching_thread_row;
use codex_state::ThreadStateAuditRow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Read;
use std::path::PathBuf;

pub(super) struct LineageAudit {
    pub(super) owned_keys: HashSet<PathBuf>,
    pub(super) retained_segments: usize,
    pub(super) errors: Vec<String>,
}

pub(super) fn audit(
    files: &[RolloutAuditFile],
    rows_by_key: &HashMap<PathBuf, Vec<&ThreadStateAuditRow>>,
) -> LineageAudit {
    let mut by_rollout_id: HashMap<&str, Vec<&RolloutAuditFile>> = HashMap::new();
    for file in files {
        by_rollout_id
            .entry(file.rollout_id.as_str())
            .or_default()
            .push(file);
    }

    let roots = files
        .iter()
        .filter(|file| has_matching_thread_row(file, rows_by_key))
        .collect::<Vec<_>>();
    let mut owned_keys = roots
        .iter()
        .map(|file| file.key.clone())
        .collect::<HashSet<_>>();
    let mut errors = Vec::new();

    for root in roots {
        let mut current = root;
        let mut seen = HashSet::new();
        loop {
            if !seen.insert(current.rollout_id.as_str()) {
                errors.push(format!("{}: history cycle", root.path.display()));
                break;
            }
            let Some(base) = current.history_base.as_ref() else {
                break;
            };
            if !current.paginated {
                errors.push(format!("{}: non-paginated history base", current.path.display()));
                break;
            }
            let base_id = base.thread_id.to_string();
            let Some(candidates) = by_rollout_id.get(base_id.as_str()) else {
                errors.push(format!("{}: missing history base {base_id}", current.path.display()));
                break;
            };
            let [parent] = candidates.as_slice() else {
                errors.push(format!("{}: ambiguous history base {base_id}", current.path.display()));
                break;
            };
            if !parent.paginated
                || current.first_ordinal != Some(base.end_ordinal_exclusive)
                || !valid_cutoff(parent, base.end_byte_offset, base.end_ordinal_exclusive)
            {
                errors.push(format!("{}: invalid history boundary", current.path.display()));
                break;
            }
            owned_keys.insert(parent.key.clone());
            current = parent;
        }
    }

    let root_count = files
        .iter()
        .filter(|file| has_matching_thread_row(file, rows_by_key))
        .count();
    LineageAudit {
        retained_segments: owned_keys.len().saturating_sub(root_count),
        owned_keys,
        errors,
    }
}

fn valid_cutoff(parent: &RolloutAuditFile, end_byte_offset: u64, end_ordinal: u64) -> bool {
    let Ok(offset) = usize::try_from(end_byte_offset) else {
        return false;
    };
    if offset == 0 || end_ordinal == 0 {
        return false;
    }
    let Ok(mut reader) = codex_rollout::open_rollout_seekable_reader(parent.path.as_path()) else {
        return false;
    };
    let mut bytes = Vec::new();
    if reader.read_to_end(&mut bytes).is_err() || offset > bytes.len() || bytes[offset - 1] != b'\n' {
        return false;
    }
    let Some(last_line) = bytes[..offset].rsplit(|byte| *byte == b'\n').nth(1) else {
        return false;
    };
    codex_rollout::parse_rollout_line_bytes(last_line)
        .is_ok_and(|line| line.ordinal == Some(end_ordinal - 1))
}
