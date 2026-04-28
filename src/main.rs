mod artnet;
mod smoother;

use clap::{Parser, ValueEnum};
use smoother::{Mode, SmootherConfig, UnknownPolicy};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Smooth bursty Art-Net UDP streams for Timber controllers"
)]
struct Cli {
    #[arg(long, default_value = "0.0.0.0:6454")]
    listen: SocketAddr,

    #[arg(long)]
    target: SocketAddr,

    #[arg(long, value_enum, default_value_t = CliMode::SyncDelay)]
    mode: CliMode,

    #[arg(long, default_value_t = 40.0)]
    fps: f64,

    #[arg(long)]
    stats: bool,

    #[arg(long)]
    universes: Option<String>,

    #[arg(long)]
    min_packet_gap_us: Option<u64>,

    #[arg(long)]
    max_pps: Option<u64>,

    #[arg(long, value_enum, default_value_t = CliUnknownPolicy::Forward)]
    unknown: CliUnknownPolicy,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliMode {
    Passthrough,
    Spread,
    SyncDelay,
    FixedFps,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliUnknownPolicy {
    Forward,
    Drop,
}

fn main() -> std::io::Result<()> {
    let cli = Cli::parse();
    init_logging();

    let config = SmootherConfig {
        listen: cli.listen,
        target: cli.target,
        mode: cli.mode.into(),
        fps: cli.fps,
        stats: cli.stats,
        universes: parse_universe_allowlist(cli.universes.as_deref())?,
        min_packet_gap: packet_gap(cli.min_packet_gap_us, cli.max_pps),
        unknown_policy: cli.unknown.into(),
    };

    smoother::run(config)
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn packet_gap(min_packet_gap_us: Option<u64>, max_pps: Option<u64>) -> Option<Duration> {
    let explicit = min_packet_gap_us.map(Duration::from_micros);
    let from_pps = max_pps.filter(|pps| *pps > 0).map(|pps| {
        let micros = 1_000_000u64.div_ceil(pps);
        Duration::from_micros(micros)
    });

    match (explicit, from_pps) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn parse_universe_allowlist(input: Option<&str>) -> std::io::Result<Option<HashSet<u16>>> {
    let Some(input) = input else {
        return Ok(None);
    };

    let mut universes = HashSet::new();
    for part in input.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        if let Some((start, end)) = part.split_once('-') {
            let start = parse_universe(start)?;
            let end = parse_universe(end)?;
            if start > end {
                return Err(invalid_input(format!("invalid universe range '{part}'")));
            }

            for universe in start..=end {
                universes.insert(universe);
            }
        } else {
            universes.insert(parse_universe(part)?);
        }
    }

    Ok(Some(universes))
}

fn parse_universe(value: &str) -> std::io::Result<u16> {
    value
        .trim()
        .parse::<u16>()
        .map_err(|_| invalid_input(format!("invalid universe '{value}'")))
}

fn invalid_input(message: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

impl From<CliMode> for Mode {
    fn from(value: CliMode) -> Self {
        match value {
            CliMode::Passthrough => Mode::Passthrough,
            CliMode::Spread => Mode::Spread,
            CliMode::SyncDelay => Mode::SyncDelay,
            CliMode::FixedFps => Mode::FixedFps,
        }
    }
}

impl From<CliUnknownPolicy> for UnknownPolicy {
    fn from(value: CliUnknownPolicy) -> Self {
        match value {
            CliUnknownPolicy::Forward => UnknownPolicy::Forward,
            CliUnknownPolicy::Drop => UnknownPolicy::Drop,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_universe_ranges() {
        let universes = parse_universe_allowlist(Some("10-12,20")).unwrap().unwrap();
        assert!(universes.contains(&10));
        assert!(universes.contains(&11));
        assert!(universes.contains(&12));
        assert!(universes.contains(&20));
        assert!(!universes.contains(&13));
    }

    #[test]
    fn packet_gap_uses_larger_gap() {
        assert_eq!(
            packet_gap(Some(100), Some(2_000)),
            Some(Duration::from_micros(500))
        );
    }
}
