//! `--signal` spec for a Slurm batch submission.
//!
//! References:
//! - <https://slurm.schedmd.com/sbatch.html> (`--signal`)
//!
//! Slurm BNF: `[R:]<sig_num|sig_name>[@<sig_time>]`
//! - `R:` prefix — also signal a job that already had the signal queued
//!   (allow re-signal during an overlapping reservation)
//! - `sig_num` — POSIX signal number (1..=64)
//! - `sig_name` — `SIGINT`, `SIGTERM`, `SIGKILL`, `USR1`, etc.
//! - `@<sig_time>` — seconds before time limit to send the signal (1..=65535)

use crate::error::SchemaParseError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignalIdent {
    Number(u8),
    Name(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlurmSignalSpec {
    pub allow_resignal: bool,
    pub signal: SignalIdent,
    pub seconds_before_end: Option<u16>,
}

impl std::fmt::Display for SignalIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SignalIdent::Number(n) => write!(f, "{n}"),
            SignalIdent::Name(name) => f.write_str(name),
        }
    }
}

impl std::fmt::Display for SlurmSignalSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.allow_resignal {
            f.write_str("R:")?;
        }
        std::fmt::Display::fmt(&self.signal, f)?;
        if let Some(sec) = self.seconds_before_end {
            write!(f, "@{sec}")?;
        }
        Ok(())
    }
}

fn parse_signal_ident(s: &str) -> Result<SignalIdent, SchemaParseError> {
    let err = || SchemaParseError::ParseError {
        key: "signal/identifier".to_string(),
        value: s.to_string(),
    };
    if s.is_empty() {
        return Err(err());
    }
    // Numeric form: must parse as u8 in 1..=64
    if s.chars().all(|c| c.is_ascii_digit()) {
        let n: u8 = s.parse().map_err(|_| err())?;
        if !(1..=64).contains(&n) {
            return Err(err());
        }
        return Ok(SignalIdent::Number(n));
    }
    // Name form: ^[A-Z][A-Z0-9_]*$
    let mut chars = s.chars();
    let first = chars.next().ok_or_else(err)?;
    if !first.is_ascii_uppercase() {
        return Err(err());
    }
    for c in chars {
        if !(c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_') {
            return Err(err());
        }
    }
    Ok(SignalIdent::Name(s.to_string()))
}

impl std::str::FromStr for SlurmSignalSpec {
    type Err = SchemaParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || SchemaParseError::ParseError {
            key: "signal".to_string(),
            value: s.to_string(),
        };

        if s.is_empty() {
            return Err(err());
        }

        let (allow_resignal, rest) = if let Some(stripped) = s.strip_prefix("R:") {
            (true, stripped)
        } else {
            (false, s)
        };

        let (sig_part, sec_part) = match rest.split_once('@') {
            Some((l, r)) => (l, Some(r)),
            None => (rest, None),
        };

        if sig_part.is_empty() {
            return Err(err());
        }

        let signal = parse_signal_ident(sig_part)?;

        let seconds_before_end = match sec_part {
            Some(r) => {
                let n: u16 = r.parse().map_err(|_| err())?;
                if n == 0 {
                    return Err(err());
                }
                Some(n)
            }
            None => None,
        };

        Ok(Self {
            allow_resignal,
            signal,
            seconds_before_end,
        })
    }
}

impl TryFrom<&str> for SlurmSignalSpec {
    type Error = SchemaParseError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        <Self as std::str::FromStr>::from_str(s)
    }
}

impl TryFrom<String> for SlurmSignalSpec {
    type Error = SchemaParseError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        <Self as std::str::FromStr>::from_str(&s)
    }
}

impl serde::Serialize for SlurmSignalSpec {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for SlurmSignalSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SignalVisitor;

        impl<'de> serde::de::Visitor<'de> for SignalVisitor {
            type Value = SlurmSignalSpec;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(
                    "a Slurm `--signal` spec string, e.g. \"USR1\", \
                     \"SIGTERM@60\", or \"R:9@5\"",
                )
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                v.parse::<SlurmSignalSpec>().map_err(E::custom)
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(&v)
            }
        }

        deserializer.deserialize_str(SignalVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- FromStr / Display roundtrip ----

    #[test]
    fn parses_signal_name_only() {
        let s: SlurmSignalSpec = "USR1".parse().unwrap();
        assert!(!s.allow_resignal);
        assert_eq!(s.signal, SignalIdent::Name("USR1".to_string()));
        assert_eq!(s.seconds_before_end, None);
        assert_eq!(s.to_string(), "USR1");
    }

    #[test]
    fn parses_signal_number_only() {
        let s: SlurmSignalSpec = "15".parse().unwrap();
        assert_eq!(s.signal, SignalIdent::Number(15));
        assert_eq!(s.to_string(), "15");
    }

    #[test]
    fn parses_signal_with_at_seconds() {
        let s: SlurmSignalSpec = "USR1@60".parse().unwrap();
        assert_eq!(s.signal, SignalIdent::Name("USR1".to_string()));
        assert_eq!(s.seconds_before_end, Some(60));
        assert_eq!(s.to_string(), "USR1@60");
    }

    #[test]
    fn parses_signal_with_r_prefix() {
        let s: SlurmSignalSpec = "R:USR1".parse().unwrap();
        assert!(s.allow_resignal);
        assert_eq!(s.signal, SignalIdent::Name("USR1".to_string()));
        assert_eq!(s.to_string(), "R:USR1");
    }

    #[test]
    fn parses_full_form() {
        let s: SlurmSignalSpec = "R:SIGTERM@30".parse().unwrap();
        assert!(s.allow_resignal);
        assert_eq!(s.signal, SignalIdent::Name("SIGTERM".to_string()));
        assert_eq!(s.seconds_before_end, Some(30));
        assert_eq!(s.to_string(), "R:SIGTERM@30");
    }

    #[test]
    fn parses_full_form_with_number() {
        let s: SlurmSignalSpec = "R:9@5".parse().unwrap();
        assert!(s.allow_resignal);
        assert_eq!(s.signal, SignalIdent::Number(9));
        assert_eq!(s.seconds_before_end, Some(5));
        assert_eq!(s.to_string(), "R:9@5");
    }

    // ---- error cases ----

    #[test]
    fn rejects_empty_string() {
        assert!("".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_lowercase_r_prefix() {
        assert!("r:USR1".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_signal_number_zero() {
        assert!("0".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_signal_number_above_64() {
        assert!("65".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_seconds_zero() {
        assert!("USR1@0".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_seconds_overflow() {
        assert!("USR1@70000".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_empty_signal_with_r_prefix() {
        assert!("R:".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_empty_signal_with_seconds() {
        assert!("@60".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_signal_name_with_comma() {
        assert!("USR1,FOO".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_signal_name_with_lowercase() {
        // Spec: SignalIdent::Name MUST match ^[A-Z][A-Z0-9_]*$
        assert!("usr1".parse::<SlurmSignalSpec>().is_err());
    }

    #[test]
    fn rejects_signal_name_starting_with_digit() {
        // "9SIG" — first char is digit but doesn't parse as full number
        assert!("9SIG".parse::<SlurmSignalSpec>().is_err());
    }

    // ---- serde TOML roundtrip ----

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Holder {
        sig: SlurmSignalSpec,
    }

    #[test]
    fn serde_string_roundtrip() {
        let original = SlurmSignalSpec {
            allow_resignal: true,
            signal: SignalIdent::Name("USR1".to_string()),
            seconds_before_end: Some(60),
        };
        let h = Holder {
            sig: original.clone(),
        };
        let toml_text = toml::to_string(&h).unwrap();
        assert!(
            toml_text.contains(r#"sig = "R:USR1@60""#),
            "actual TOML: {toml_text}"
        );
        let back: Holder = toml::from_str(&toml_text).unwrap();
        assert_eq!(back.sig, original);
    }

    #[test]
    fn serde_rejects_invalid_string() {
        let err = toml::from_str::<Holder>(r#"sig = "r:bad""#);
        assert!(err.is_err());
    }
}
