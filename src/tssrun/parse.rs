//! Pure parsers for `salloc:` lines emitted by `tssrun` on allocation.
//!
//! These intentionally match exact prefixes — the kudpc manual prints
//! `"salloc: Granted job allocation N"` and
//! `"salloc: Nodes <node> are ready for job"`. Site-specific banner
//! changes break these parsers on purpose so the failure is visible.

/// Returns `Some(jobid)` when `line` is exactly the SLURM
/// "Granted job allocation N" message.
pub fn parse_salloc_jobid(line: &str) -> Option<u64> {
    line.strip_prefix("salloc: Granted job allocation ")
        .map(str::trim)
        .and_then(|s| s.parse::<u64>().ok())
}

/// Returns `Some(node_spec)` when `line` is the SLURM
/// "Nodes <spec> are ready for job" message. The node spec is preserved
/// verbatim (e.g. `"cnode3"` or `"cnode[3-4]"`).
pub fn parse_salloc_node(line: &str) -> Option<String> {
    let rest = line.strip_prefix("salloc: Nodes ")?;
    let (node, _) = rest.split_once(" are ready for job")?;
    Some(node.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_jobid_from_granted_line() {
        assert_eq!(
            parse_salloc_jobid("salloc: Granted job allocation 102362"),
            Some(102362)
        );
    }

    #[test]
    fn rejects_jobid_when_prefix_missing() {
        assert_eq!(parse_salloc_jobid("Granted job allocation 102362"), None);
    }

    #[test]
    fn rejects_jobid_when_value_not_numeric() {
        assert_eq!(
            parse_salloc_jobid("salloc: Granted job allocation abc"),
            None
        );
    }

    #[test]
    fn parses_node_from_ready_line() {
        assert_eq!(
            parse_salloc_node("salloc: Nodes cnode3 are ready for job"),
            Some("cnode3".to_string())
        );
    }

    #[test]
    fn parses_multi_node_form_verbatim() {
        assert_eq!(
            parse_salloc_node("salloc: Nodes cnode[3-4] are ready for job"),
            Some("cnode[3-4]".to_string())
        );
    }

    #[test]
    fn rejects_node_when_marker_absent() {
        assert_eq!(
            parse_salloc_node("salloc: Nodes cnode3 are still pending"),
            None
        );
    }
}
