//! `--array` / `-a` spec for a Slurm batch submission.
//!
//! See [`SlurmArraySpec`] for the textual form, parsing rules, and serde
//! support. Re-exported from [`crate::entities::slurm`] for backwards
//! compatibility with the rest of the schema.
//!
//! References:
//! - <https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/tips#arrayjob>
//! - <https://slurm.schedmd.com/sbatch.html>

use crate::error::SchemaParseError;

/// `--array` / `-a` spec for a Slurm batch submission.
///
/// Textual form (per Slurm and Kyoto-U KUDPC docs):
///
/// ```text
///   <entry>[,<entry>...][%<max_concurrent>]
///   <entry>      ::= <index> | <start>-<end> | <start>-<end>:<step>
/// ```
///
/// Examples:
/// - `"0-15"`         — indices 0..=15
/// - `"0-15:4"`       — 0, 4, 8, 12 (every 4th)
/// - `"0,6,16-32"`    — 0, 6, then 16..=32
/// - `"0-15%4"`       — at most 4 tasks running concurrently
///
/// Constraints enforced by parsing:
/// - all indices are non-negative (`u32`)
/// - `start <= end` for ranges
/// - `step > 0`
/// - `max_concurrent > 0`
/// - the index list is non-empty
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlurmArraySpec {
    /// Comma-separated entries in the textual form. Always non-empty.
    pub indices: Vec<ArrayIndex>,
    /// Concurrency cap from the trailing `%N` suffix (`None` = unlimited).
    pub max_concurrent: Option<u32>,
}

/// One entry of a [`SlurmArraySpec`] index list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArrayIndex {
    /// Single index, e.g. `5`.
    Single(u32),
    /// Inclusive range, e.g. `0-15`.
    Range { start: u32, end: u32 },
    /// Inclusive range with step, e.g. `0-15:4` (= 0, 4, 8, 12).
    Stepped { start: u32, end: u32, step: u32 },
}

impl std::fmt::Display for ArrayIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            ArrayIndex::Single(i) => write!(f, "{}", i),
            ArrayIndex::Range { start, end } => write!(f, "{}-{}", start, end),
            ArrayIndex::Stepped { start, end, step } => {
                write!(f, "{}-{}:{}", start, end, step)
            }
        }
    }
}

impl std::fmt::Display for SlurmArraySpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for entry in &self.indices {
            if !first {
                f.write_str(",")?;
            }
            first = false;
            std::fmt::Display::fmt(entry, f)?;
        }
        if let Some(n) = self.max_concurrent {
            write!(f, "%{}", n)?;
        }
        Ok(())
    }
}

impl std::str::FromStr for ArrayIndex {
    type Err = SchemaParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || SchemaParseError::ParseError {
            key: "array_spec/entry".to_string(),
            value: s.to_string(),
        };

        if s.is_empty() {
            return Err(err());
        }

        // Split off optional ":<step>" suffix.
        let (range_part, step) = match s.split_once(':') {
            Some((r, st)) => {
                let step = st.parse::<u32>().map_err(|_| err())?;
                if step == 0 {
                    return Err(err());
                }
                (r, Some(step))
            }
            None => (s, None),
        };

        // Parse "<start>-<end>" or single "<index>".
        match range_part.split_once('-') {
            Some((a, b)) => {
                let start = a.parse::<u32>().map_err(|_| err())?;
                let end = b.parse::<u32>().map_err(|_| err())?;
                if start > end {
                    return Err(err());
                }
                Ok(match step {
                    Some(step) => ArrayIndex::Stepped { start, end, step },
                    None => ArrayIndex::Range { start, end },
                })
            }
            None => {
                if step.is_some() {
                    // ":step" without a range is not legal.
                    return Err(err());
                }
                let i = range_part.parse::<u32>().map_err(|_| err())?;
                Ok(ArrayIndex::Single(i))
            }
        }
    }
}

impl std::str::FromStr for SlurmArraySpec {
    type Err = SchemaParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || SchemaParseError::ParseError {
            key: "array_spec".to_string(),
            value: s.to_string(),
        };

        if s.is_empty() {
            return Err(err());
        }

        // Split off optional "%<max_concurrent>" suffix.
        let (indices_part, max_concurrent) = match s.split_once('%') {
            Some((idx, cap)) => {
                let n = cap.parse::<u32>().map_err(|_| err())?;
                if n == 0 {
                    return Err(err());
                }
                (idx, Some(n))
            }
            None => (s, None),
        };

        if indices_part.is_empty() {
            return Err(err());
        }

        let indices = indices_part
            .split(',')
            .map(|e| e.parse::<ArrayIndex>())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            indices,
            max_concurrent,
        })
    }
}

impl TryFrom<&str> for SlurmArraySpec {
    type Error = SchemaParseError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        <Self as std::str::FromStr>::from_str(s)
    }
}

impl TryFrom<String> for SlurmArraySpec {
    type Error = SchemaParseError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        <Self as std::str::FromStr>::from_str(&s)
    }
}

impl serde::Serialize for SlurmArraySpec {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for SlurmArraySpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SpecVisitor;

        impl<'de> serde::de::Visitor<'de> for SpecVisitor {
            type Value = SlurmArraySpec;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(
                    "a Slurm `--array` spec string, e.g. \"0-15:4%2\" \
                     or \"0,6,16-32\"",
                )
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                v.parse::<SlurmArraySpec>().map_err(E::custom)
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(&v)
            }
        }

        deserializer.deserialize_str(SpecVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- FromStr / Display roundtrip ----

    #[test]
    fn parses_single_index() {
        let s: SlurmArraySpec = "5".parse().unwrap();
        assert_eq!(
            s,
            SlurmArraySpec {
                indices: vec![ArrayIndex::Single(5)],
                max_concurrent: None,
            }
        );
        assert_eq!(s.to_string(), "5");
    }

    #[test]
    fn parses_simple_range() {
        let s: SlurmArraySpec = "0-15".parse().unwrap();
        assert_eq!(
            s,
            SlurmArraySpec {
                indices: vec![ArrayIndex::Range { start: 0, end: 15 }],
                max_concurrent: None,
            }
        );
        assert_eq!(s.to_string(), "0-15");
    }

    #[test]
    fn parses_stepped_range() {
        let s: SlurmArraySpec = "0-15:4".parse().unwrap();
        assert_eq!(
            s,
            SlurmArraySpec {
                indices: vec![ArrayIndex::Stepped {
                    start: 0,
                    end: 15,
                    step: 4
                }],
                max_concurrent: None,
            }
        );
        assert_eq!(s.to_string(), "0-15:4");
    }

    #[test]
    fn parses_concurrency_cap() {
        let s: SlurmArraySpec = "0-15%4".parse().unwrap();
        assert_eq!(
            s,
            SlurmArraySpec {
                indices: vec![ArrayIndex::Range { start: 0, end: 15 }],
                max_concurrent: Some(4),
            }
        );
        assert_eq!(s.to_string(), "0-15%4");
    }

    #[test]
    fn parses_mixed_list_with_step_and_cap() {
        let s: SlurmArraySpec = "0,6,16-32:2%3".parse().unwrap();
        assert_eq!(
            s,
            SlurmArraySpec {
                indices: vec![
                    ArrayIndex::Single(0),
                    ArrayIndex::Single(6),
                    ArrayIndex::Stepped {
                        start: 16,
                        end: 32,
                        step: 2
                    },
                ],
                max_concurrent: Some(3),
            }
        );
        assert_eq!(s.to_string(), "0,6,16-32:2%3");
    }

    #[test]
    fn kudpc_example_kyoto_doc() {
        // Example from KUDPC docs: -a 1-3
        let s: SlurmArraySpec = "1-3".parse().unwrap();
        assert_eq!(s.to_string(), "1-3");
        // Step example: 1-5:2
        let s: SlurmArraySpec = "1-5:2".parse().unwrap();
        assert_eq!(
            s,
            SlurmArraySpec {
                indices: vec![ArrayIndex::Stepped {
                    start: 1,
                    end: 5,
                    step: 2
                }],
                max_concurrent: None,
            }
        );
    }

    // ---- Rejection ----

    #[test]
    fn rejects_empty_string() {
        assert!("".parse::<SlurmArraySpec>().is_err());
    }

    #[test]
    fn rejects_reversed_range() {
        assert!("5-3".parse::<SlurmArraySpec>().is_err());
    }

    #[test]
    fn rejects_zero_step() {
        assert!("0-15:0".parse::<SlurmArraySpec>().is_err());
    }

    #[test]
    fn rejects_zero_concurrency_cap() {
        assert!("0-15%0".parse::<SlurmArraySpec>().is_err());
    }

    #[test]
    fn rejects_step_without_range() {
        assert!("5:2".parse::<SlurmArraySpec>().is_err());
    }

    #[test]
    fn rejects_negative_index() {
        assert!("-5".parse::<SlurmArraySpec>().is_err());
    }

    #[test]
    fn rejects_dangling_range() {
        assert!("0-".parse::<SlurmArraySpec>().is_err());
        assert!("-".parse::<SlurmArraySpec>().is_err());
    }

    #[test]
    fn rejects_only_concurrency() {
        assert!("%4".parse::<SlurmArraySpec>().is_err());
    }

    // ---- serde roundtrip via TOML ----

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Holder {
        spec: SlurmArraySpec,
    }

    #[test]
    fn deserialize_from_toml_string() {
        let h: Holder = toml::from_str(r#"spec = "0-15:4%2""#).unwrap();
        assert_eq!(
            h.spec,
            SlurmArraySpec {
                indices: vec![ArrayIndex::Stepped {
                    start: 0,
                    end: 15,
                    step: 4
                }],
                max_concurrent: Some(2),
            }
        );
    }

    #[test]
    fn serialize_to_toml_string() {
        let h = Holder {
            spec: SlurmArraySpec {
                indices: vec![
                    ArrayIndex::Single(0),
                    ArrayIndex::Range { start: 4, end: 8 },
                ],
                max_concurrent: Some(2),
            },
        };
        let out = toml::to_string(&h).unwrap();
        assert!(out.contains(r#"spec = "0,4-8%2""#), "actual TOML: {out}");
    }

    #[test]
    fn toml_roundtrip_preserves_value() {
        let original = SlurmArraySpec {
            indices: vec![
                ArrayIndex::Single(0),
                ArrayIndex::Single(6),
                ArrayIndex::Stepped {
                    start: 16,
                    end: 32,
                    step: 2,
                },
            ],
            max_concurrent: Some(3),
        };
        let h = Holder {
            spec: original.clone(),
        };
        let toml_text = toml::to_string(&h).unwrap();
        let back: Holder = toml::from_str(&toml_text).unwrap();
        assert_eq!(back.spec, original);
    }

    #[test]
    fn deserialize_rejects_bad_string() {
        let err = toml::from_str::<Holder>(r#"spec = "0-15:0""#);
        assert!(err.is_err());
    }
}
