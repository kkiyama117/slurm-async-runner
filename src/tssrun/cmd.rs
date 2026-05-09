//! Spec types: [`Resource`] and `TssrunCmd` with argv builder. No I/O.

/// Resource spec passed to `tssrun --rsc p=:t=:c=:m=:g=`.
///
/// All fields are optional; `render` only emits keys whose value is `Some`.
/// Order is fixed: `p`, `t`, `c`, `m`, `g`. The `memory` field is a free
/// string (`"2G"`, `"512M"`, etc.) since SLURM accepts unit suffixes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resource {
    pub processes: Option<u32>,
    pub threads: Option<u32>,
    pub cores: Option<u32>,
    pub memory: Option<String>,
    pub gpus: Option<u32>,
}

impl Resource {
    /// Renders the colon-joined `p=…:t=…:m=…:g=…` string.
    /// Returns `None` if every field is `None`.
    pub fn render(&self) -> Option<String> {
        let mut parts: Vec<String> = Vec::with_capacity(5);
        if let Some(p) = self.processes {
            parts.push(format!("p={p}"));
        }
        if let Some(t) = self.threads {
            parts.push(format!("t={t}"));
        }
        if let Some(c) = self.cores {
            parts.push(format!("c={c}"));
        }
        if let Some(m) = &self.memory {
            parts.push(format!("m={m}"));
        }
        if let Some(g) = self.gpus {
            parts.push(format!("g={g}"));
        }
        if parts.is_empty() {
            None
        } else {
            Some(parts.join(":"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_default_renders_none() {
        assert_eq!(Resource::default().render(), None);
    }

    #[test]
    fn resource_full_renders_in_order() {
        let r = Resource {
            processes: Some(4),
            threads: Some(8),
            cores: Some(8),
            memory: Some("2G".into()),
            gpus: Some(1),
        };
        assert_eq!(r.render().as_deref(), Some("p=4:t=8:c=8:m=2G:g=1"));
    }

    #[test]
    fn resource_partial_skips_none_keys() {
        let r = Resource {
            processes: Some(4),
            memory: Some("2G".into()),
            ..Default::default()
        };
        assert_eq!(r.render().as_deref(), Some("p=4:m=2G"));
    }
}
