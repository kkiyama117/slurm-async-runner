//! `--dependency` / `-d` spec for a Slurm batch submission.
//!
//! See [`SlurmDependency`] for the textual form, parsing rules, and serde
//! support. Re-exported from [`crate::entities::slurm`] for backwards
//! compatibility with the rest of the schema.
//!
//! References:
//! - <https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/tips#dependency>
//! - <https://slurm.schedmd.com/sbatch.html> (`--dependency`)

use std::fmt::Write as _;

use crate::error::SchemaParseError;

/// Kind of one dependency clause.
///
/// KUDPC documents the first four (`after` / `afterany` / `afterok` /
/// `afternotok`); the remaining variants are documented by Slurm itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DependencyType {
    /// Begin after the listed jobs have started (or been cancelled).
    /// Each job_id may carry a `+<minutes>` delay.
    After,
    /// Begin after the listed jobs terminate (any exit status).
    AfterAny,
    /// Begin after the listed jobs terminate and their burst-buffer stage-out
    /// has finished.
    AfterBurstBuffer,
    /// For array jobs: each task starts after the corresponding task in the
    /// referenced array completes successfully.
    AfterCorr,
    /// Begin after the listed jobs terminate in a failed state.
    AfterNotOk,
    /// Begin after the listed jobs terminate successfully.
    AfterOk,
    /// Begin only after any other job with the same name and user has
    /// terminated. Takes no job_ids.
    Singleton,
}

impl DependencyType {
    /// Slurm keyword used in the textual form.
    pub fn as_keyword(self) -> &'static str {
        match self {
            DependencyType::After => "after",
            DependencyType::AfterAny => "afterany",
            DependencyType::AfterBurstBuffer => "afterburstbuffer",
            DependencyType::AfterCorr => "aftercorr",
            DependencyType::AfterNotOk => "afternotok",
            DependencyType::AfterOk => "afterok",
            DependencyType::Singleton => "singleton",
        }
    }

    /// `singleton` is the only variant that does not accept any job_ids.
    fn takes_jobs(self) -> bool {
        !matches!(self, DependencyType::Singleton)
    }
}

impl std::str::FromStr for DependencyType {
    type Err = SchemaParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "after" => Ok(DependencyType::After),
            "afterany" => Ok(DependencyType::AfterAny),
            "afterburstbuffer" => Ok(DependencyType::AfterBurstBuffer),
            "aftercorr" => Ok(DependencyType::AfterCorr),
            "afternotok" => Ok(DependencyType::AfterNotOk),
            "afterok" => Ok(DependencyType::AfterOk),
            "singleton" => Ok(DependencyType::Singleton),
            _ => Err(SchemaParseError::ParseError {
                key: "dependency/type".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl std::fmt::Display for DependencyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_keyword())
    }
}

/// One job_id reference in a dependency clause.
///
/// `delay_minutes` is only meaningful for [`DependencyType::After`] (Slurm
/// allows `after:<job_id>+<minutes>`); parsing rejects it for other types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DependencyJobRef {
    pub job_id: u32,
    pub delay_minutes: Option<u32>,
}

impl DependencyJobRef {
    pub fn new(job_id: u32) -> Self {
        Self {
            job_id,
            delay_minutes: None,
        }
    }
}

impl std::fmt::Display for DependencyJobRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.delay_minutes {
            Some(m) => write!(f, "{}+{}", self.job_id, m),
            None => write!(f, "{}", self.job_id),
        }
    }
}

/// One dependency clause: a [`DependencyType`] and (for non-`singleton`
/// types) a non-empty list of job_ids.
///
/// Textual form: `<type>` for `singleton`, otherwise `<type>:<id>[+<min>][:<id>...]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencyClause {
    pub dep_type: DependencyType,
    pub job_refs: Vec<DependencyJobRef>,
}

impl std::fmt::Display for DependencyClause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.dep_type.as_keyword())?;
        for r in &self.job_refs {
            write!(f, ":{}", r)?;
        }
        Ok(())
    }
}

/// How clauses inside a [`SlurmDependency`] are combined.
///
/// Slurm forbids mixing `,` and `?` in a single `--dependency` value; a spec
/// with only one clause is rendered with [`DependencyJoin::And`] by default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DependencyJoin {
    /// Comma `,` — every clause must be satisfied.
    And,
    /// Question mark `?` — any clause being satisfied releases the job.
    Or,
}

impl DependencyJoin {
    fn as_char(self) -> char {
        match self {
            DependencyJoin::And => ',',
            DependencyJoin::Or => '?',
        }
    }
}

/// `--dependency` / `-d` spec for a Slurm batch submission.
///
/// Textual form (per Slurm and Kyoto-U KUDPC docs):
///
/// ```text
///   <clause>[<sep><clause>...]
///   <clause> ::= <type>[:<job_id>[+<minutes>][:<job_id>...]]
///   <type>   ::= after | afterany | afterburstbuffer | aftercorr
///              | afternotok | afterok | singleton
///   <sep>    ::= "," (AND) | "?" (OR)   — must not mix in one spec
/// ```
///
/// Examples:
/// - `"afterok:200"`              — KUDPC manual example
/// - `"afterok:200:201"`          — wait for both 200 and 201 to succeed
/// - `"afterok:200,afterany:201"` — 200 must succeed AND 201 must finish
/// - `"afterok:200?afterany:201"` — either 200 succeeds OR 201 finishes
/// - `"after:200+5"`              — start 5 min after job 200 begins
/// - `"singleton"`                — no other same-named job of mine running
///
/// Constraints enforced by parsing:
/// - the clause list is non-empty
/// - `,` and `?` are not mixed in one spec
/// - non-`singleton` clauses have at least one job_id
/// - `singleton` clauses have no job_ids
/// - `+<minutes>` delay is only allowed on `after:` clauses
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlurmDependency {
    pub clauses: Vec<DependencyClause>,
    pub join: DependencyJoin,
}

impl std::fmt::Display for SlurmDependency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sep = self.join.as_char();
        let mut first = true;
        for c in &self.clauses {
            if !first {
                f.write_char(sep)?;
            }
            first = false;
            std::fmt::Display::fmt(c, f)?;
        }
        Ok(())
    }
}

fn parse_job_ref(s: &str, allow_delay: bool) -> Result<DependencyJobRef, SchemaParseError> {
    let err = || SchemaParseError::ParseError {
        key: "dependency/job_id".to_string(),
        value: s.to_string(),
    };
    if s.is_empty() {
        return Err(err());
    }
    match s.split_once('+') {
        Some((id, delay)) => {
            if !allow_delay {
                return Err(err());
            }
            let job_id = id.parse::<u32>().map_err(|_| err())?;
            let delay_minutes = delay.parse::<u32>().map_err(|_| err())?;
            Ok(DependencyJobRef {
                job_id,
                delay_minutes: Some(delay_minutes),
            })
        }
        None => {
            let job_id = s.parse::<u32>().map_err(|_| err())?;
            Ok(DependencyJobRef {
                job_id,
                delay_minutes: None,
            })
        }
    }
}

fn parse_clause(s: &str) -> Result<DependencyClause, SchemaParseError> {
    let err = || SchemaParseError::ParseError {
        key: "dependency/clause".to_string(),
        value: s.to_string(),
    };
    if s.is_empty() {
        return Err(err());
    }

    let mut parts = s.split(':');
    let head = parts.next().ok_or_else(err)?;
    let dep_type = head.parse::<DependencyType>()?;

    let raw_jobs: Vec<&str> = parts.collect();

    if !dep_type.takes_jobs() {
        if !raw_jobs.is_empty() {
            return Err(err());
        }
        return Ok(DependencyClause {
            dep_type,
            job_refs: Vec::new(),
        });
    }

    if raw_jobs.is_empty() {
        return Err(err());
    }

    let allow_delay = matches!(dep_type, DependencyType::After);
    let job_refs = raw_jobs
        .into_iter()
        .map(|j| parse_job_ref(j, allow_delay))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(DependencyClause { dep_type, job_refs })
}

impl std::str::FromStr for SlurmDependency {
    type Err = SchemaParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || SchemaParseError::ParseError {
            key: "dependency".to_string(),
            value: s.to_string(),
        };

        if s.is_empty() {
            return Err(err());
        }

        let has_and = s.contains(',');
        let has_or = s.contains('?');
        if has_and && has_or {
            return Err(err());
        }

        let (raw_clauses, join): (Vec<&str>, DependencyJoin) = if has_or {
            (s.split('?').collect(), DependencyJoin::Or)
        } else {
            (s.split(',').collect(), DependencyJoin::And)
        };

        let clauses = raw_clauses
            .into_iter()
            .map(parse_clause)
            .collect::<Result<Vec<_>, _>>()?;

        if clauses.is_empty() {
            return Err(err());
        }

        Ok(Self { clauses, join })
    }
}

impl TryFrom<&str> for SlurmDependency {
    type Error = SchemaParseError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        <Self as std::str::FromStr>::from_str(s)
    }
}

impl TryFrom<String> for SlurmDependency {
    type Error = SchemaParseError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        <Self as std::str::FromStr>::from_str(&s)
    }
}

impl serde::Serialize for SlurmDependency {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for SlurmDependency {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct DepVisitor;

        impl<'de> serde::de::Visitor<'de> for DepVisitor {
            type Value = SlurmDependency;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(
                    "a Slurm `--dependency` spec string, e.g. \"afterok:200\", \
                     \"afterok:200,afterany:201\", or \"singleton\"",
                )
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                v.parse::<SlurmDependency>().map_err(E::custom)
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(&v)
            }
        }

        deserializer.deserialize_str(DepVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- FromStr / Display roundtrip ----

    #[test]
    fn parses_kudpc_example_afterok_200() {
        let d: SlurmDependency = "afterok:200".parse().unwrap();
        assert_eq!(
            d,
            SlurmDependency {
                clauses: vec![DependencyClause {
                    dep_type: DependencyType::AfterOk,
                    job_refs: vec![DependencyJobRef::new(200)],
                }],
                join: DependencyJoin::And,
            }
        );
        assert_eq!(d.to_string(), "afterok:200");
    }

    #[test]
    fn parses_each_kudpc_dependency_type() {
        for kw in ["after", "afterany", "afterok", "afternotok"] {
            let raw = format!("{kw}:42");
            let d: SlurmDependency = raw.parse().unwrap();
            assert_eq!(d.to_string(), raw);
        }
    }

    #[test]
    fn parses_multiple_job_ids_in_one_clause() {
        let d: SlurmDependency = "afterok:200:201:202".parse().unwrap();
        assert_eq!(d.clauses.len(), 1);
        assert_eq!(d.clauses[0].job_refs.len(), 3);
        assert_eq!(d.to_string(), "afterok:200:201:202");
    }

    #[test]
    fn parses_and_joined_clauses() {
        let d: SlurmDependency = "afterok:200,afterany:201".parse().unwrap();
        assert_eq!(d.join, DependencyJoin::And);
        assert_eq!(d.clauses.len(), 2);
        assert_eq!(d.to_string(), "afterok:200,afterany:201");
    }

    #[test]
    fn parses_or_joined_clauses() {
        let d: SlurmDependency = "afterok:200?afterany:201".parse().unwrap();
        assert_eq!(d.join, DependencyJoin::Or);
        assert_eq!(d.clauses.len(), 2);
        assert_eq!(d.to_string(), "afterok:200?afterany:201");
    }

    #[test]
    fn parses_after_with_delay() {
        let d: SlurmDependency = "after:200+5".parse().unwrap();
        assert_eq!(
            d,
            SlurmDependency {
                clauses: vec![DependencyClause {
                    dep_type: DependencyType::After,
                    job_refs: vec![DependencyJobRef {
                        job_id: 200,
                        delay_minutes: Some(5),
                    }],
                }],
                join: DependencyJoin::And,
            }
        );
        assert_eq!(d.to_string(), "after:200+5");
    }

    #[test]
    fn parses_singleton() {
        let d: SlurmDependency = "singleton".parse().unwrap();
        assert_eq!(
            d,
            SlurmDependency {
                clauses: vec![DependencyClause {
                    dep_type: DependencyType::Singleton,
                    job_refs: vec![],
                }],
                join: DependencyJoin::And,
            }
        );
        assert_eq!(d.to_string(), "singleton");
    }

    #[test]
    fn parses_aftercorr_and_afterburstbuffer() {
        let d: SlurmDependency = "aftercorr:7:8".parse().unwrap();
        assert_eq!(d.clauses[0].dep_type, DependencyType::AfterCorr);
        assert_eq!(d.to_string(), "aftercorr:7:8");

        let d: SlurmDependency = "afterburstbuffer:9".parse().unwrap();
        assert_eq!(d.clauses[0].dep_type, DependencyType::AfterBurstBuffer);
        assert_eq!(d.to_string(), "afterburstbuffer:9");
    }

    // ---- Rejection ----

    #[test]
    fn rejects_empty_string() {
        assert!("".parse::<SlurmDependency>().is_err());
    }

    #[test]
    fn rejects_unknown_type() {
        assert!("afternever:200".parse::<SlurmDependency>().is_err());
    }

    #[test]
    fn rejects_mixed_and_or_separators() {
        assert!(
            "afterok:200,afterany:201?afterok:202"
                .parse::<SlurmDependency>()
                .is_err()
        );
    }

    #[test]
    fn rejects_clause_with_no_job_id() {
        assert!("afterok".parse::<SlurmDependency>().is_err());
        assert!("afterok:".parse::<SlurmDependency>().is_err());
    }

    #[test]
    fn rejects_singleton_with_job_id() {
        assert!("singleton:200".parse::<SlurmDependency>().is_err());
    }

    #[test]
    fn rejects_delay_on_non_after_type() {
        // `+<minutes>` is only legal for the `after` type per Slurm docs.
        assert!("afterok:200+5".parse::<SlurmDependency>().is_err());
    }

    #[test]
    fn rejects_non_numeric_job_id() {
        assert!("afterok:abc".parse::<SlurmDependency>().is_err());
    }

    #[test]
    fn rejects_dangling_separator() {
        assert!("afterok:200,".parse::<SlurmDependency>().is_err());
        assert!(",afterok:200".parse::<SlurmDependency>().is_err());
    }

    // ---- serde roundtrip via TOML ----

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Holder {
        dep: SlurmDependency,
    }

    #[test]
    fn deserialize_from_toml_string() {
        let h: Holder = toml::from_str(r#"dep = "afterok:200""#).unwrap();
        assert_eq!(
            h.dep,
            SlurmDependency {
                clauses: vec![DependencyClause {
                    dep_type: DependencyType::AfterOk,
                    job_refs: vec![DependencyJobRef::new(200)],
                }],
                join: DependencyJoin::And,
            }
        );
    }

    #[test]
    fn serialize_to_toml_string() {
        let h = Holder {
            dep: SlurmDependency {
                clauses: vec![
                    DependencyClause {
                        dep_type: DependencyType::AfterOk,
                        job_refs: vec![DependencyJobRef::new(200)],
                    },
                    DependencyClause {
                        dep_type: DependencyType::AfterAny,
                        job_refs: vec![DependencyJobRef::new(201)],
                    },
                ],
                join: DependencyJoin::And,
            },
        };
        let out = toml::to_string(&h).unwrap();
        assert!(
            out.contains(r#"dep = "afterok:200,afterany:201""#),
            "actual TOML: {out}"
        );
    }

    #[test]
    fn toml_roundtrip_preserves_value() {
        let original = SlurmDependency {
            clauses: vec![
                DependencyClause {
                    dep_type: DependencyType::After,
                    job_refs: vec![DependencyJobRef {
                        job_id: 200,
                        delay_minutes: Some(5),
                    }],
                },
                DependencyClause {
                    dep_type: DependencyType::AfterOk,
                    job_refs: vec![DependencyJobRef::new(201), DependencyJobRef::new(202)],
                },
            ],
            join: DependencyJoin::Or,
        };
        let h = Holder {
            dep: original.clone(),
        };
        let toml_text = toml::to_string(&h).unwrap();
        let back: Holder = toml::from_str(&toml_text).unwrap();
        assert_eq!(back.dep, original);
    }

    #[test]
    fn deserialize_rejects_bad_string() {
        let err = toml::from_str::<Holder>(r#"dep = "afterok""#);
        assert!(err.is_err());
    }
}
