//! `--time` / `-t` spec for a Slurm batch submission.
//!
//! See [`JobTimeLimit`] for the textual form, parsing rules, and serde
//! support. Re-exported from [`crate::entities::slurm`] for backwards
//! compatibility with the rest of the schema.
//!
//! References:
//! - <https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/batch#slurm>
//! - <https://slurm.schedmd.com/sbatch.html> (`--time`)

use std::num::NonZeroU32;

use chrono::TimeDelta;

use crate::error::SchemaParseError;

/// `--time` / `-t` wall-clock limit for a Slurm batch submission.
///
/// Stored as a positive number of seconds. Slurm's `sbatch(1)` accepts six
/// surface forms for the `--time` argument:
///
/// ```text
///   <minutes>
///   <minutes>:<seconds>
///   <hours>:<minutes>:<seconds>
///   <days>-<hours>
///   <days>-<hours>:<minutes>
///   <days>-<hours>:<minutes>:<seconds>
/// ```
///
/// All six are accepted by [`std::str::FromStr`]; [`std::fmt::Display`] always emits the
/// canonical `HH:MM:SS` form (the hour field is allowed to exceed 23 — Slurm
/// itself accepts that).
///
/// Note on the two-component form: per Slurm, `"5:30"` means *5 minutes
/// 30 seconds* (not 5 hours 30 minutes). Round-tripping clarifies this:
/// `"5:30".parse::<JobTimeLimit>().unwrap().to_string() == "00:05:30"`.
///
/// Examples (verbatim from the KUDPC manual / Slurm docs):
/// - `"01:00:00"`   — one hour
/// - `"24:00:00"`   — 24 hours
/// - `"3-12:00:00"` — three days and twelve hours
///
/// Constraints enforced by parsing:
/// - the input is non-empty
/// - every numeric component is a non-negative `u32`
/// - the resulting total duration is strictly positive (Slurm treats
///   `--time=0` as "unlimited"; if you want an unlimited job, omit
///   `time_limit` entirely instead)
/// - the total fits in [`u32`] seconds (~136 years)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JobTimeLimit {
    seconds: NonZeroU32,
}

impl JobTimeLimit {
    /// Construct from a positive total-seconds count.
    pub fn from_seconds(seconds: NonZeroU32) -> Self {
        Self { seconds }
    }

    /// Total duration in seconds.
    pub fn total_seconds(self) -> u32 {
        self.seconds.get()
    }

    /// Hour component of the canonical `HH:MM:SS` form (may exceed 23).
    pub fn hours(self) -> u32 {
        self.seconds.get() / 3600
    }

    /// Minute component of the canonical `HH:MM:SS` form (always `0..60`).
    pub fn minutes(self) -> u32 {
        (self.seconds.get() % 3600) / 60
    }

    /// Second component of the canonical `HH:MM:SS` form (always `0..60`).
    pub fn seconds_part(self) -> u32 {
        self.seconds.get() % 60
    }
}

impl std::fmt::Display for JobTimeLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:02}:{:02}:{:02}",
            self.hours(),
            self.minutes(),
            self.seconds_part()
        )
    }
}

fn parse_component(raw: &str, original: &str) -> Result<u32, SchemaParseError> {
    raw.parse::<u32>()
        .map_err(|_| SchemaParseError::ParseError {
            key: "time_limit".to_string(),
            value: original.to_string(),
        })
}

impl std::str::FromStr for JobTimeLimit {
    type Err = SchemaParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || SchemaParseError::ParseError {
            key: "time_limit".to_string(),
            value: s.to_string(),
        };

        if s.is_empty() {
            return Err(err());
        }

        // The presence of a `-` selects the days-form; per Slurm's grammar a
        // dash is only legal as the days/hours separator, so we match on the
        // first occurrence and require the days field to be a bare integer.
        let total: u64 = if let Some((days_s, rest)) = s.split_once('-') {
            if days_s.is_empty() || rest.is_empty() {
                return Err(err());
            }
            let days = parse_component(days_s, s)?;
            let parts: Vec<&str> = rest.split(':').collect();
            let (h, m, sec) = match parts.as_slice() {
                [h] => (parse_component(h, s)?, 0, 0),
                [h, m] => (parse_component(h, s)?, parse_component(m, s)?, 0),
                [h, m, sec] => (
                    parse_component(h, s)?,
                    parse_component(m, s)?,
                    parse_component(sec, s)?,
                ),
                _ => return Err(err()),
            };
            (days as u64) * 86_400 + (h as u64) * 3_600 + (m as u64) * 60 + (sec as u64)
        } else {
            let parts: Vec<&str> = s.split(':').collect();
            match parts.as_slice() {
                [m] => (parse_component(m, s)? as u64) * 60,
                [m, sec] => (parse_component(m, s)? as u64) * 60 + parse_component(sec, s)? as u64,
                [h, m, sec] => {
                    (parse_component(h, s)? as u64) * 3_600
                        + (parse_component(m, s)? as u64) * 60
                        + parse_component(sec, s)? as u64
                }
                _ => return Err(err()),
            }
        };

        let secs: u32 = total.try_into().map_err(|_| err())?;
        let value = NonZeroU32::new(secs).ok_or_else(err)?;
        Ok(Self { seconds: value })
    }
}

impl TryFrom<&str> for JobTimeLimit {
    type Error = SchemaParseError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        <Self as std::str::FromStr>::from_str(s)
    }
}

impl TryFrom<String> for JobTimeLimit {
    type Error = SchemaParseError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        <Self as std::str::FromStr>::from_str(&s)
    }
}

impl TryFrom<TimeDelta> for JobTimeLimit {
    type Error = SchemaParseError;

    fn try_from(value: TimeDelta) -> Result<Self, Self::Error> {
        let err = || SchemaParseError::ParseError {
            key: "time_limit".to_string(),
            value: value.to_string(),
        };
        let total = value.num_seconds();
        if total <= 0 {
            return Err(err());
        }
        let secs: u32 = total.try_into().map_err(|_| err())?;
        let value = NonZeroU32::new(secs).ok_or_else(err)?;
        Ok(Self { seconds: value })
    }
}

impl From<JobTimeLimit> for TimeDelta {
    fn from(value: JobTimeLimit) -> Self {
        // `seconds` is a u32 so it always fits in the i64 that `TimeDelta`
        // takes; this conversion is therefore infallible.
        TimeDelta::seconds(i64::from(value.seconds.get()))
    }
}

impl serde::Serialize for JobTimeLimit {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for JobTimeLimit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct LimitVisitor;

        impl<'de> serde::de::Visitor<'de> for LimitVisitor {
            type Value = JobTimeLimit;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(
                    "a Slurm `--time` spec string, e.g. \"01:00:00\", \"24:00:00\", \
                     or \"3-12:00:00\"",
                )
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                v.parse::<JobTimeLimit>().map_err(E::custom)
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(&v)
            }
        }

        deserializer.deserialize_str(LimitVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nz(n: u32) -> NonZeroU32 {
        NonZeroU32::new(n).unwrap()
    }

    fn from_secs(n: u32) -> JobTimeLimit {
        JobTimeLimit::from_seconds(nz(n))
    }

    // ---- FromStr / Display roundtrip ----

    #[test]
    fn parses_kudpc_one_hour() {
        let t: JobTimeLimit = "01:00:00".parse().unwrap();
        assert_eq!(t, from_secs(3600));
        assert_eq!(t.to_string(), "01:00:00");
    }

    #[test]
    fn parses_h_m_s_form() {
        let t: JobTimeLimit = "12:34:56".parse().unwrap();
        assert_eq!(t.total_seconds(), 12 * 3600 + 34 * 60 + 56);
        assert_eq!(t.to_string(), "12:34:56");
    }

    #[test]
    fn parses_minutes_only() {
        // Slurm: bare integer = minutes
        let t: JobTimeLimit = "30".parse().unwrap();
        assert_eq!(t.total_seconds(), 30 * 60);
        assert_eq!(t.to_string(), "00:30:00");
    }

    #[test]
    fn parses_minutes_seconds_form() {
        // Slurm: 2-part = minutes:seconds (NOT hours:minutes)
        let t: JobTimeLimit = "5:30".parse().unwrap();
        assert_eq!(t.total_seconds(), 5 * 60 + 30);
        assert_eq!(t.to_string(), "00:05:30");
    }

    #[test]
    fn parses_days_hours_form() {
        let t: JobTimeLimit = "1-0".parse().unwrap();
        assert_eq!(t.total_seconds(), 24 * 3600);
        assert_eq!(t.to_string(), "24:00:00");
    }

    #[test]
    fn parses_days_hours_minutes_form() {
        let t: JobTimeLimit = "2-3:30".parse().unwrap();
        assert_eq!(t.total_seconds(), 2 * 86_400 + 3 * 3600 + 30 * 60);
        assert_eq!(t.to_string(), "51:30:00");
    }

    #[test]
    fn parses_days_hours_minutes_seconds_form() {
        let t: JobTimeLimit = "3-12:00:00".parse().unwrap();
        assert_eq!(t.total_seconds(), 3 * 86_400 + 12 * 3600);
        assert_eq!(t.to_string(), "84:00:00");
    }

    #[test]
    fn display_emits_canonical_hms_for_24h_plus() {
        let t = from_secs(25 * 3600);
        assert_eq!(t.to_string(), "25:00:00");
        let again: JobTimeLimit = t.to_string().parse().unwrap();
        assert_eq!(again, t);
    }

    #[test]
    fn accepts_minute_overflow_field() {
        // `00:90:00` -> 90 minutes -> 1h30m. Slurm accepts this leniently;
        // we follow suit.
        let t: JobTimeLimit = "00:90:00".parse().unwrap();
        assert_eq!(t.total_seconds(), 90 * 60);
        assert_eq!(t.to_string(), "01:30:00");
    }

    // ---- Component accessors ----

    #[test]
    fn component_accessors_match_canonical_form() {
        let t = from_secs(12 * 3600 + 34 * 60 + 56);
        assert_eq!(t.hours(), 12);
        assert_eq!(t.minutes(), 34);
        assert_eq!(t.seconds_part(), 56);
    }

    // ---- Rejection ----

    #[test]
    fn rejects_empty_string() {
        assert!("".parse::<JobTimeLimit>().is_err());
    }

    #[test]
    fn rejects_zero_minutes() {
        // `0` would parse as 0 minutes -> unlimited per Slurm. We require
        // callers to omit the field instead.
        assert!("0".parse::<JobTimeLimit>().is_err());
    }

    #[test]
    fn rejects_all_zero_hms() {
        assert!("00:00:00".parse::<JobTimeLimit>().is_err());
    }

    #[test]
    fn rejects_all_zero_days_form() {
        assert!("0-0".parse::<JobTimeLimit>().is_err());
        assert!("0-0:0:0".parse::<JobTimeLimit>().is_err());
    }

    #[test]
    fn rejects_too_many_colon_parts() {
        assert!("1:2:3:4".parse::<JobTimeLimit>().is_err());
    }

    #[test]
    fn rejects_dangling_dash() {
        assert!("-".parse::<JobTimeLimit>().is_err());
        assert!("5-".parse::<JobTimeLimit>().is_err());
        assert!("-5".parse::<JobTimeLimit>().is_err());
    }

    #[test]
    fn rejects_dangling_colon() {
        assert!(":".parse::<JobTimeLimit>().is_err());
        assert!("1:".parse::<JobTimeLimit>().is_err());
        assert!(":1".parse::<JobTimeLimit>().is_err());
    }

    #[test]
    fn rejects_non_numeric_components() {
        assert!("a".parse::<JobTimeLimit>().is_err());
        assert!("1:b".parse::<JobTimeLimit>().is_err());
        assert!("1-2:c".parse::<JobTimeLimit>().is_err());
    }

    #[test]
    fn rejects_negative_components() {
        // Negative numbers are not parseable as u32.
        assert!("-1:00:00".parse::<JobTimeLimit>().is_err());
        assert!("1:-1:0".parse::<JobTimeLimit>().is_err());
    }

    #[test]
    fn rejects_two_dashes() {
        // Per Slurm grammar a dash is only legal once.
        assert!("1-2-3".parse::<JobTimeLimit>().is_err());
    }

    // ---- TimeDelta interop ----

    #[test]
    fn try_from_timedelta_accepts_positive() {
        let td = TimeDelta::hours(2) + TimeDelta::minutes(30);
        let t = JobTimeLimit::try_from(td).unwrap();
        assert_eq!(t.total_seconds(), 2 * 3600 + 30 * 60);
    }

    #[test]
    fn try_from_timedelta_rejects_zero() {
        assert!(JobTimeLimit::try_from(TimeDelta::zero()).is_err());
    }

    #[test]
    fn try_from_timedelta_rejects_negative() {
        assert!(JobTimeLimit::try_from(TimeDelta::seconds(-1)).is_err());
    }

    #[test]
    fn into_timedelta_round_trips() {
        let original = from_secs(12 * 3600 + 34 * 60 + 56);
        let td: TimeDelta = original.into();
        let back = JobTimeLimit::try_from(td).unwrap();
        assert_eq!(back, original);
    }

    // ---- serde roundtrip via TOML ----

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Holder {
        time_limit: JobTimeLimit,
    }

    #[test]
    fn deserialize_from_toml_string() {
        let h: Holder = toml::from_str(r#"time_limit = "01:30:00""#).unwrap();
        assert_eq!(h.time_limit.total_seconds(), 3600 + 30 * 60);
    }

    #[test]
    fn deserialize_minutes_form() {
        let h: Holder = toml::from_str(r#"time_limit = "45""#).unwrap();
        assert_eq!(h.time_limit.total_seconds(), 45 * 60);
    }

    #[test]
    fn deserialize_days_form() {
        let h: Holder = toml::from_str(r#"time_limit = "1-12:00:00""#).unwrap();
        assert_eq!(h.time_limit.total_seconds(), 86_400 + 12 * 3600);
    }

    #[test]
    fn serialize_to_toml_canonical_form() {
        let h = Holder {
            time_limit: from_secs(7200),
        };
        let out = toml::to_string(&h).unwrap();
        assert!(
            out.contains(r#"time_limit = "02:00:00""#),
            "actual TOML: {out}"
        );
    }

    #[test]
    fn toml_roundtrip_preserves_value() {
        let original = from_secs(3 * 86_400 + 12 * 3600 + 34 * 60 + 56);
        let h = Holder {
            time_limit: original,
        };
        let toml_text = toml::to_string(&h).unwrap();
        let back: Holder = toml::from_str(&toml_text).unwrap();
        assert_eq!(back.time_limit, original);
    }

    #[test]
    fn deserialize_rejects_bad_string() {
        assert!(toml::from_str::<Holder>(r#"time_limit = """#).is_err());
        assert!(toml::from_str::<Holder>(r#"time_limit = "0""#).is_err());
        assert!(toml::from_str::<Holder>(r#"time_limit = "1:2:3:4""#).is_err());
        assert!(toml::from_str::<Holder>(r#"time_limit = "abc""#).is_err());
    }
}
