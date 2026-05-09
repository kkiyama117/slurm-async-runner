//! `--rsc` / resource-list spec for a Slurm batch submission.
//!
//! See [`ResourceSpec`] for the textual form, parsing rules, and serde
//! support. Re-exported from [`crate::entities::slurm`] for backwards
//! compatibility with the rest of the schema.
//!
//! References:
//! - <https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/batch#slurm>
//! - <https://slurm.schedmd.com/sbatch.html>

use std::num::NonZeroU32;

use crate::error::SchemaParseError;

/// Memory size suffix recognised by Slurm `--mem` and KUDPC `--rsc m=`.
///
/// Slurm documents the suffixes `[K|M|G|T]`. A unit-less integer is treated
/// as megabytes (the Slurm default for `--mem`). The original suffix is
/// preserved on parse so [`std::fmt::Display`] can emit the same shape the user
/// supplied (modulo the `Mega` default, which is rendered as `M`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemoryUnit {
    /// `K` — kibibytes.
    Kilo,
    /// `M` — mebibytes. Default when the user supplies no suffix.
    Mega,
    /// `G` — gibibytes.
    Giga,
    /// `T` — tebibytes.
    Tera,
}

impl MemoryUnit {
    fn as_char(self) -> char {
        match self {
            MemoryUnit::Kilo => 'K',
            MemoryUnit::Mega => 'M',
            MemoryUnit::Giga => 'G',
            MemoryUnit::Tera => 'T',
        }
    }

    fn from_char(c: char) -> Option<Self> {
        match c {
            'K' => Some(MemoryUnit::Kilo),
            'M' => Some(MemoryUnit::Mega),
            'G' => Some(MemoryUnit::Giga),
            'T' => Some(MemoryUnit::Tera),
            _ => None,
        }
    }
}

/// Memory size for a Slurm `m=` token.
///
/// The value is a positive [`NonZeroU32`] of [`MemoryUnit`] units. Slurm
/// itself rejects `m=0`, so the type encodes that invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Memory {
    pub value: NonZeroU32,
    pub unit: MemoryUnit,
}

impl std::fmt::Display for Memory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.value, self.unit.as_char())
    }
}

impl std::str::FromStr for Memory {
    type Err = SchemaParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || SchemaParseError::ParseError {
            key: "resource_spec/m".to_string(),
            value: s.to_string(),
        };

        if s.is_empty() {
            return Err(err());
        }

        // Last char may be a unit suffix; otherwise the whole string is the
        // numeric value and the unit defaults to mebibytes (Slurm default
        // for `--mem`).
        let last = s.chars().next_back().ok_or_else(err)?;
        let (digits, unit) = if last.is_ascii_digit() {
            (s, MemoryUnit::Mega)
        } else {
            let unit = MemoryUnit::from_char(last).ok_or_else(err)?;
            (&s[..s.len() - last.len_utf8()], unit)
        };

        if digits.is_empty() {
            return Err(err());
        }
        let raw: u32 = digits.parse().map_err(|_| err())?;
        let value = NonZeroU32::new(raw).ok_or_else(err)?;

        Ok(Self { value, unit })
    }
}

/// `[slurm].resource_spec` — Slurm `--rsc`-style resource list.
///
/// Textual form is a colon-separated list of `key=value` tokens (per the
/// KUDPC manual). Two flavours are recognised:
///
/// ```text
///   CPU:  "p=<u32>:t=<u32>:c=<u32>:m=<memory>"
///   GPU:  "g=<u32>"
/// ```
///
/// The presence of a `g=` token selects the GPU flavour; otherwise the spec
/// is parsed as CPU and all four of `p`, `t`, `c`, `m` are required. Token
/// order is irrelevant to the parser, but [`std::fmt::Display`] always emits the
/// canonical order shown above.
///
/// Examples (verbatim from the KUDPC manual):
/// - `"p=4:t=8:c=8:m=8G"` — hybrid parallel CPU job
/// - `"g=1"`              — single-GPU job
///
/// Constraints enforced by parsing:
/// - the token list is non-empty
/// - tokens are well-formed `key=value` pairs
/// - keys are not duplicated
/// - CPU flavour: exactly the keys `{p, t, c, m}` are present
/// - GPU flavour: exactly the key `{g}` is present (mixing CPU and GPU keys
///   in one spec is rejected)
/// - integer counts are positive (`NonZeroU32`); `p=0`, `t=0`, `c=0`, `g=0`
///   are rejected
/// - `m` is parsed as [`Memory`] (positive integer with optional `K|M|G|T`
///   suffix; unit-less defaults to mebibytes per Slurm `--mem` semantics)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceSpec {
    /// CPU resource request — `p` procs, `t` threads, `c` cores, `m` memory.
    CPU(ResourceSpecCPU),
    /// GPU resource request — `g` GPUs.
    GPU(ResourceSpecGPU),
}

/// CPU flavour of [`ResourceSpec`].
///
/// Per the KUDPC manual
/// (<https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/resource#rscoption>),
/// each of `p`, `t`, `c`, `m` is individually optional — when omitted
/// the system applies its default (1 for the integer fields,
/// system-dependent for memory). All-`None` is permitted and renders
/// to an empty string via [`std::fmt::Display`]; consumers (e.g.
/// `tssrun`'s argv builder) treat that as "skip the `--rsc` flag
/// entirely".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceSpecCPU {
    /// `p=` — number of MPI processes when set; `None` means "use
    /// the system default" (typically 1). Always `>= 1` when present.
    pub p: Option<NonZeroU32>,
    /// `t=` — threads per process. Same `Option` semantics as `p`.
    pub t: Option<NonZeroU32>,
    /// `c=` — cores per process. Same `Option` semantics as `p`.
    pub c: Option<NonZeroU32>,
    /// `m=` — memory request. Same `Option` semantics as `p`.
    pub m: Option<Memory>,
}

/// GPU flavour of [`ResourceSpec`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceSpecGPU {
    /// `g=` — number of GPUs (>= 1).
    pub g: NonZeroU32,
}

impl std::fmt::Display for ResourceSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceSpec::CPU(c) => {
                let mut parts: Vec<String> = Vec::with_capacity(4);
                if let Some(p) = c.p {
                    parts.push(format!("p={p}"));
                }
                if let Some(t) = c.t {
                    parts.push(format!("t={t}"));
                }
                if let Some(cc) = c.c {
                    parts.push(format!("c={cc}"));
                }
                if let Some(m) = &c.m {
                    parts.push(format!("m={m}"));
                }
                write!(f, "{}", parts.join(":"))
            }
            ResourceSpec::GPU(g) => write!(f, "g={}", g.g),
        }
    }
}

fn parse_count(key: &str, raw: &str, original: &str) -> Result<NonZeroU32, SchemaParseError> {
    let err = || SchemaParseError::ParseError {
        key: format!("resource_spec/{key}"),
        value: original.to_string(),
    };
    let n: u32 = raw.parse().map_err(|_| err())?;
    NonZeroU32::new(n).ok_or_else(err)
}

impl std::str::FromStr for ResourceSpec {
    type Err = SchemaParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || SchemaParseError::ParseError {
            key: "resource_spec".to_string(),
            value: s.to_string(),
        };

        if s.is_empty() {
            return Err(err());
        }

        let mut p: Option<NonZeroU32> = None;
        let mut t: Option<NonZeroU32> = None;
        let mut c: Option<NonZeroU32> = None;
        let mut m: Option<Memory> = None;
        let mut g: Option<NonZeroU32> = None;

        for token in s.split(':') {
            let (key, value) = token.split_once('=').ok_or_else(err)?;
            if key.is_empty() || value.is_empty() {
                return Err(err());
            }
            match key {
                "p" => {
                    if p.is_some() {
                        return Err(err());
                    }
                    p = Some(parse_count("p", value, s)?);
                }
                "t" => {
                    if t.is_some() {
                        return Err(err());
                    }
                    t = Some(parse_count("t", value, s)?);
                }
                "c" => {
                    if c.is_some() {
                        return Err(err());
                    }
                    c = Some(parse_count("c", value, s)?);
                }
                "m" => {
                    if m.is_some() {
                        return Err(err());
                    }
                    m = Some(value.parse::<Memory>()?);
                }
                "g" => {
                    if g.is_some() {
                        return Err(err());
                    }
                    g = Some(parse_count("g", value, s)?);
                }
                _ => return Err(err()),
            }
        }

        match (g, p, t, c, m) {
            // GPU flavour: exactly `g`, no CPU keys.
            (Some(g), None, None, None, None) => Ok(ResourceSpec::GPU(ResourceSpecGPU { g })),

            // GPU mixed with any CPU key — rejected.
            (Some(_), _, _, _, _) => Err(err()),

            // CPU flavour: any non-empty subset of (p, t, c, m).
            (None, p, t, c, m) if p.is_some() || t.is_some() || c.is_some() || m.is_some() => {
                Ok(ResourceSpec::CPU(ResourceSpecCPU { p, t, c, m }))
            }

            // Empty (no recognised keys) — already caught earlier by
            // the empty-string guard, but keep an explicit fall-through.
            _ => Err(err()),
        }
    }
}

impl TryFrom<&str> for ResourceSpec {
    type Error = SchemaParseError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        <Self as std::str::FromStr>::from_str(s)
    }
}

impl TryFrom<String> for ResourceSpec {
    type Error = SchemaParseError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        <Self as std::str::FromStr>::from_str(&s)
    }
}

impl serde::Serialize for ResourceSpec {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for ResourceSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SpecVisitor;

        impl<'de> serde::de::Visitor<'de> for SpecVisitor {
            type Value = ResourceSpec;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(
                    "a Slurm `--rsc` spec string, e.g. \"p=4:t=8:c=8:m=8G\" \
                     or \"g=1\"",
                )
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                v.parse::<ResourceSpec>().map_err(E::custom)
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

    fn nz(n: u32) -> NonZeroU32 {
        NonZeroU32::new(n).unwrap()
    }

    fn mem(value: u32, unit: MemoryUnit) -> Memory {
        Memory {
            value: nz(value),
            unit,
        }
    }

    // ---- Memory parsing ----

    #[test]
    fn memory_parses_each_suffix() {
        assert_eq!("4K".parse::<Memory>().unwrap(), mem(4, MemoryUnit::Kilo));
        assert_eq!("4M".parse::<Memory>().unwrap(), mem(4, MemoryUnit::Mega));
        assert_eq!("8G".parse::<Memory>().unwrap(), mem(8, MemoryUnit::Giga));
        assert_eq!("1T".parse::<Memory>().unwrap(), mem(1, MemoryUnit::Tera));
    }

    #[test]
    fn memory_unitless_defaults_to_mega() {
        assert_eq!(
            "1024".parse::<Memory>().unwrap(),
            mem(1024, MemoryUnit::Mega)
        );
    }

    #[test]
    fn memory_display_round_trips() {
        for (raw, canonical) in [
            ("4K", "4K"),
            ("8G", "8G"),
            // unit-less input renders as `M`, which still re-parses
            ("1024", "1024M"),
        ] {
            let m: Memory = raw.parse().unwrap();
            assert_eq!(m.to_string(), canonical);
            let again: Memory = m.to_string().parse().unwrap();
            assert_eq!(m, again);
        }
    }

    #[test]
    fn memory_rejects_zero() {
        assert!("0G".parse::<Memory>().is_err());
        assert!("0".parse::<Memory>().is_err());
    }

    #[test]
    fn memory_rejects_unknown_suffix() {
        assert!("4X".parse::<Memory>().is_err());
        assert!("4g".parse::<Memory>().is_err()); // case-sensitive per KUDPC examples
    }

    #[test]
    fn memory_rejects_empty_or_suffix_only() {
        assert!("".parse::<Memory>().is_err());
        assert!("G".parse::<Memory>().is_err());
    }

    // ---- ResourceSpec FromStr / Display roundtrip ----

    #[test]
    fn parses_kudpc_cpu_example() {
        let r: ResourceSpec = "p=4:t=8:c=8:m=8G".parse().unwrap();
        assert_eq!(
            r,
            ResourceSpec::CPU(ResourceSpecCPU {
                p: Some(nz(4)),
                t: Some(nz(8)),
                c: Some(nz(8)),
                m: Some(mem(8, MemoryUnit::Giga)),
            })
        );
        assert_eq!(r.to_string(), "p=4:t=8:c=8:m=8G");
    }

    #[test]
    fn parses_cpu_spec_in_arbitrary_order() {
        let r: ResourceSpec = "m=56G:c=56:t=56:p=1".parse().unwrap();
        assert_eq!(
            r,
            ResourceSpec::CPU(ResourceSpecCPU {
                p: Some(nz(1)),
                t: Some(nz(56)),
                c: Some(nz(56)),
                m: Some(mem(56, MemoryUnit::Giga)),
            })
        );
        // Display always emits canonical order.
        assert_eq!(r.to_string(), "p=1:t=56:c=56:m=56G");
    }

    #[test]
    fn parses_kudpc_gpu_example() {
        let r: ResourceSpec = "g=1".parse().unwrap();
        assert_eq!(r, ResourceSpec::GPU(ResourceSpecGPU { g: nz(1) }));
        assert_eq!(r.to_string(), "g=1");
    }

    #[test]
    fn parses_unitless_memory_in_cpu_spec() {
        let r: ResourceSpec = "p=1:t=1:c=1:m=1024".parse().unwrap();
        if let ResourceSpec::CPU(c) = r {
            assert_eq!(c.m, Some(mem(1024, MemoryUnit::Mega)));
        } else {
            panic!("expected CPU variant");
        }
    }

    // ---- Rejection ----

    #[test]
    fn rejects_empty_string() {
        assert!("".parse::<ResourceSpec>().is_err());
    }

    #[test]
    fn rejects_mixed_cpu_and_gpu_keys() {
        assert!("p=1:t=1:c=1:m=1G:g=1".parse::<ResourceSpec>().is_err());
        assert!("g=1:p=1".parse::<ResourceSpec>().is_err());
    }

    #[test]
    fn rejects_unknown_keys() {
        assert!("p=1:t=1:c=1:m=1G:x=1".parse::<ResourceSpec>().is_err());
    }

    #[test]
    fn rejects_duplicate_keys() {
        assert!("p=1:p=2:t=1:c=1:m=1G".parse::<ResourceSpec>().is_err());
    }

    #[test]
    fn rejects_zero_counts() {
        assert!("p=0:t=1:c=1:m=1G".parse::<ResourceSpec>().is_err());
        assert!("p=1:t=0:c=1:m=1G".parse::<ResourceSpec>().is_err());
        assert!("p=1:t=1:c=0:m=1G".parse::<ResourceSpec>().is_err());
        assert!("g=0".parse::<ResourceSpec>().is_err());
    }

    #[test]
    fn rejects_zero_memory() {
        assert!("p=1:t=1:c=1:m=0G".parse::<ResourceSpec>().is_err());
    }

    #[test]
    fn rejects_invalid_memory_unit() {
        assert!("p=1:t=1:c=1:m=1X".parse::<ResourceSpec>().is_err());
    }

    #[test]
    fn rejects_missing_equals_in_token() {
        assert!("p1:t=1:c=1:m=1G".parse::<ResourceSpec>().is_err());
    }

    #[test]
    fn rejects_empty_value() {
        assert!("p=:t=1:c=1:m=1G".parse::<ResourceSpec>().is_err());
        assert!("g=".parse::<ResourceSpec>().is_err());
    }

    #[test]
    fn rejects_dangling_separator() {
        assert!("p=1:t=1:c=1:m=1G:".parse::<ResourceSpec>().is_err());
        assert!(":p=1:t=1:c=1:m=1G".parse::<ResourceSpec>().is_err());
    }

    // ---- serde roundtrip via TOML ----

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Holder {
        rsc: ResourceSpec,
    }

    #[test]
    fn deserialize_cpu_from_toml_string() {
        let h: Holder = toml::from_str(r#"rsc = "p=4:t=8:c=8:m=8G""#).unwrap();
        assert_eq!(
            h.rsc,
            ResourceSpec::CPU(ResourceSpecCPU {
                p: Some(nz(4)),
                t: Some(nz(8)),
                c: Some(nz(8)),
                m: Some(mem(8, MemoryUnit::Giga)),
            })
        );
    }

    #[test]
    fn deserialize_gpu_from_toml_string() {
        let h: Holder = toml::from_str(r#"rsc = "g=4""#).unwrap();
        assert_eq!(h.rsc, ResourceSpec::GPU(ResourceSpecGPU { g: nz(4) }));
    }

    #[test]
    fn serialize_cpu_to_toml_string() {
        let h = Holder {
            rsc: ResourceSpec::CPU(ResourceSpecCPU {
                p: Some(nz(1)),
                t: Some(nz(56)),
                c: Some(nz(56)),
                m: Some(mem(56, MemoryUnit::Giga)),
            }),
        };
        let out = toml::to_string(&h).unwrap();
        assert!(
            out.contains(r#"rsc = "p=1:t=56:c=56:m=56G""#),
            "actual TOML: {out}"
        );
    }

    #[test]
    fn serialize_gpu_to_toml_string() {
        let h = Holder {
            rsc: ResourceSpec::GPU(ResourceSpecGPU { g: nz(8) }),
        };
        let out = toml::to_string(&h).unwrap();
        assert!(out.contains(r#"rsc = "g=8""#), "actual TOML: {out}");
    }

    #[test]
    fn toml_roundtrip_preserves_value() {
        let original = ResourceSpec::CPU(ResourceSpecCPU {
            p: Some(nz(4)),
            t: Some(nz(8)),
            c: Some(nz(32)),
            m: Some(mem(128, MemoryUnit::Giga)),
        });
        let h = Holder {
            rsc: original.clone(),
        };
        let toml_text = toml::to_string(&h).unwrap();
        let back: Holder = toml::from_str(&toml_text).unwrap();
        assert_eq!(back.rsc, original);
    }

    #[test]
    fn deserialize_rejects_bad_string() {
        assert!(toml::from_str::<Holder>(r#"rsc = "p=0:t=1:c=1:m=1G""#).is_err());
        assert!(toml::from_str::<Holder>(r#"rsc = "p=1:t=1:c=1:m=1X""#).is_err());
    }

    #[test]
    fn parses_kudpc_p60_t1_c1_example() {
        // From the KUDPC manual: an MPI 60-way partial CPU spec.
        let r: ResourceSpec = "p=60:t=1:c=1".parse().unwrap();
        assert_eq!(
            r,
            ResourceSpec::CPU(ResourceSpecCPU {
                p: Some(nz(60)),
                t: Some(nz(1)),
                c: Some(nz(1)),
                m: None,
            })
        );
        assert_eq!(r.to_string(), "p=60:t=1:c=1");
    }

    #[test]
    fn parses_partial_cpu_spec_m_only() {
        let r: ResourceSpec = "m=8G".parse().unwrap();
        if let ResourceSpec::CPU(c) = r {
            assert_eq!(c.p, None);
            assert_eq!(c.t, None);
            assert_eq!(c.c, None);
            assert_eq!(c.m, Some(mem(8, MemoryUnit::Giga)));
        } else {
            panic!("expected CPU variant");
        }
    }

    #[test]
    fn display_round_trips_partial_cpu() {
        let original = ResourceSpec::CPU(ResourceSpecCPU {
            p: Some(nz(60)),
            t: Some(nz(1)),
            c: Some(nz(1)),
            m: None,
        });
        let s = original.to_string();
        let parsed: ResourceSpec = s.parse().unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn cpu_default_is_all_none_and_display_is_empty() {
        let r = ResourceSpec::CPU(ResourceSpecCPU::default());
        assert_eq!(r.to_string(), "");
    }
}
