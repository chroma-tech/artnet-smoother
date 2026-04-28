use crate::artnet::{self, PacketKind};
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::HashSet;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

const MAX_PACKET_SIZE: usize = 1500;
const SPIN_WINDOW: Duration = Duration::from_micros(150);
const SHORT_SLEEP_WINDOW: Duration = Duration::from_millis(2);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    Passthrough,
    Spread,
    SyncDelay,
    FixedFps,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnknownPolicy {
    Forward,
    Drop,
}

#[derive(Clone, Debug)]
pub struct SmootherConfig {
    pub listen: SocketAddr,
    pub target: SocketAddr,
    pub mode: Mode,
    pub fps: f64,
    pub stats: bool,
    pub universes: Option<HashSet<u16>>,
    pub min_packet_gap: Option<Duration>,
    pub unknown_policy: UnknownPolicy,
}

#[derive(Default)]
struct Stats {
    input_packets: AtomicU64,
    output_packets: AtomicU64,
    input_dmx: AtomicU64,
    input_sync: AtomicU64,
    dropped_packets: AtomicU64,
    late_frames: AtomicU64,
    queued_packets: AtomicUsize,
    frame_rate_milli_hz: AtomicU64,
}

#[derive(Debug)]
struct Packet {
    bytes: Vec<u8>,
    received_at: Instant,
}

#[derive(Debug)]
enum Event {
    Dmx(Packet),
    Sync(Packet),
}

pub fn run(config: SmootherConfig) -> io::Result<()> {
    validate_config(&config)?;

    let listen_socket = bind_udp(config.listen)?;
    let target_socket = UdpSocket::bind(if config.target.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    })?;
    target_socket.connect(config.target)?;

    info!(
        listen = %config.listen,
        target = %config.target,
        mode = ?config.mode,
        fps = config.fps,
        "starting smoother"
    );

    let stats = Arc::new(Stats::default());
    let min_gap = config.min_packet_gap.unwrap_or(Duration::ZERO);

    if config.stats {
        spawn_stats_thread(Arc::clone(&stats));
    }

    if config.mode == Mode::Passthrough {
        receive_passthrough(listen_socket, target_socket, config, stats)
    } else {
        let (tx, rx) = mpsc::channel();
        let engine_stats = Arc::clone(&stats);
        let engine_config = config.clone();
        let engine_socket = target_socket.try_clone()?;

        thread::spawn(move || {
            if let Err(err) = run_engine(engine_config, engine_socket, rx, engine_stats, min_gap) {
                warn!(error = %err, "pacing engine exited");
            }
        });

        receive_buffered(listen_socket, target_socket, config, stats, tx)
    }
}

fn validate_config(config: &SmootherConfig) -> io::Result<()> {
    if !config.fps.is_finite() || config.fps <= 0.0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--fps must be a positive finite number",
        ));
    }

    Ok(())
}

fn bind_udp(addr: SocketAddr) -> io::Result<UdpSocket> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.bind(&addr.into())?;
    Ok(socket.into())
}

fn receive_passthrough(
    listen_socket: UdpSocket,
    target_socket: UdpSocket,
    config: SmootherConfig,
    stats: Arc<Stats>,
) -> io::Result<()> {
    let mut buf = [0u8; MAX_PACKET_SIZE];
    loop {
        let (len, _) = listen_socket.recv_from(&mut buf)?;
        let kind = record_input(&stats, &buf[..len]);
        if should_forward_immediately(kind, &config) {
            send_packet(&target_socket, &buf[..len], &stats)?;
        } else {
            stats.dropped_packets.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn receive_buffered(
    listen_socket: UdpSocket,
    target_socket: UdpSocket,
    config: SmootherConfig,
    stats: Arc<Stats>,
    tx: mpsc::Sender<Event>,
) -> io::Result<()> {
    let mut buf = [0u8; MAX_PACKET_SIZE];
    loop {
        let (len, _) = listen_socket.recv_from(&mut buf)?;
        let packet = &buf[..len];
        let kind = record_input(&stats, packet);

        match kind {
            PacketKind::ArtDmx { universe, .. } => {
                if let Some(universes) = &config.universes
                    && !universes.contains(&universe)
                {
                    debug!(universe, "dropping ArtDmx outside universe allowlist");
                    stats.dropped_packets.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                stats.queued_packets.fetch_add(1, Ordering::Relaxed);
                let event = Event::Dmx(Packet {
                    bytes: packet.to_vec(),
                    received_at: Instant::now(),
                });
                if tx.send(event).is_err() {
                    return Ok(());
                }
            }
            PacketKind::ArtSync if config.mode == Mode::SyncDelay => {
                let event = Event::Sync(Packet {
                    bytes: packet.to_vec(),
                    received_at: Instant::now(),
                });
                if tx.send(event).is_err() {
                    return Ok(());
                }
            }
            PacketKind::ArtSync if config.mode == Mode::FixedFps => {
                debug!("ignoring ArtSync in fixed-fps mode");
            }
            PacketKind::ArtSync => {
                send_packet(&target_socket, packet, &stats)?;
            }
            other if should_forward_immediately(other, &config) => {
                send_packet(&target_socket, packet, &stats)?;
            }
            _ => {
                stats.dropped_packets.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn run_engine(
    config: SmootherConfig,
    target_socket: UdpSocket,
    rx: mpsc::Receiver<Event>,
    stats: Arc<Stats>,
    min_gap: Duration,
) -> io::Result<()> {
    match config.mode {
        Mode::Spread | Mode::FixedFps => {
            run_fixed_interval_engine(config, target_socket, rx, stats, min_gap)
        }
        Mode::SyncDelay => run_sync_delay_engine(config, target_socket, rx, stats, min_gap),
        Mode::Passthrough => Ok(()),
    }
}

fn run_sync_delay_engine(
    config: SmootherConfig,
    target_socket: UdpSocket,
    rx: mpsc::Receiver<Event>,
    stats: Arc<Stats>,
    min_gap: Duration,
) -> io::Result<()> {
    let default_interval = frame_interval(config.fps);
    let mut current_frame = Vec::new();
    let mut last_sync_at: Option<Instant> = None;
    let mut interval = default_interval;
    let mut last_send_at: Option<Instant> = None;

    while let Ok(event) = rx.recv() {
        match event {
            Event::Dmx(packet) => current_frame.push(packet),
            Event::Sync(sync) => {
                if let Some(previous_sync) = last_sync_at {
                    interval = smooth_interval(
                        interval,
                        sync.received_at.saturating_duration_since(previous_sync),
                    );
                    stats
                        .frame_rate_milli_hz
                        .store(duration_to_milli_hz(interval), Ordering::Relaxed);
                }
                last_sync_at = Some(sync.received_at);

                let frame = std::mem::take(&mut current_frame);
                pace_frame(
                    &target_socket,
                    frame,
                    Some(sync),
                    interval,
                    min_gap,
                    &mut last_send_at,
                    &stats,
                )?;
            }
        }
    }

    Ok(())
}

fn run_fixed_interval_engine(
    config: SmootherConfig,
    target_socket: UdpSocket,
    rx: mpsc::Receiver<Event>,
    stats: Arc<Stats>,
    min_gap: Duration,
) -> io::Result<()> {
    let interval = frame_interval(config.fps);
    stats
        .frame_rate_milli_hz
        .store(duration_to_milli_hz(interval), Ordering::Relaxed);
    let mut current_frame = Vec::new();
    let mut next_flush = Instant::now() + interval;
    let mut last_send_at: Option<Instant> = None;

    loop {
        let now = Instant::now();
        let timeout = next_flush.saturating_duration_since(now);
        match rx.recv_timeout(timeout) {
            Ok(Event::Dmx(packet)) => current_frame.push(packet),
            Ok(Event::Sync(_)) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let frame = std::mem::take(&mut current_frame);
                pace_frame(
                    &target_socket,
                    frame,
                    None,
                    interval,
                    min_gap,
                    &mut last_send_at,
                    &stats,
                )?;
                next_flush += interval;
                while next_flush <= Instant::now() {
                    stats.late_frames.fetch_add(1, Ordering::Relaxed);
                    next_flush += interval;
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
        }
    }
}

fn pace_frame(
    target_socket: &UdpSocket,
    mut frame: Vec<Packet>,
    sync: Option<Packet>,
    interval: Duration,
    min_gap: Duration,
    last_send_at: &mut Option<Instant>,
    stats: &Stats,
) -> io::Result<()> {
    if frame.is_empty() {
        if let Some(sync) = sync {
            wait_for_gap(min_gap, last_send_at);
            send_packet(target_socket, &sync.bytes, stats)?;
            *last_send_at = Some(Instant::now());
        }
        return Ok(());
    }

    let frame_start = Instant::now();
    let packets_len = frame.len();
    let spacing = interval.div_f64(packets_len as f64);

    for (index, packet) in frame.drain(..).enumerate() {
        let deadline = frame_start + spacing.mul_f64(index as f64);
        wait_until(deadline);
        wait_for_gap(min_gap, last_send_at);
        send_packet(target_socket, &packet.bytes, stats)?;
        *last_send_at = Some(Instant::now());
        stats.queued_packets.fetch_sub(1, Ordering::Relaxed);
    }

    let sync_deadline = frame_start + interval;
    wait_until(sync_deadline);
    if let Some(sync) = sync {
        wait_for_gap(min_gap, last_send_at);
        send_packet(target_socket, &sync.bytes, stats)?;
        *last_send_at = Some(Instant::now());
    }

    if Instant::now() > frame_start + interval + Duration::from_millis(1) {
        stats.late_frames.fetch_add(1, Ordering::Relaxed);
    }

    Ok(())
}

fn record_input(stats: &Stats, packet: &[u8]) -> PacketKind {
    let kind = artnet::classify(packet);
    stats.input_packets.fetch_add(1, Ordering::Relaxed);
    match kind {
        PacketKind::ArtDmx { .. } => {
            stats.input_dmx.fetch_add(1, Ordering::Relaxed);
        }
        PacketKind::ArtSync => {
            stats.input_sync.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
    kind
}

fn should_forward_immediately(kind: PacketKind, config: &SmootherConfig) -> bool {
    match kind {
        PacketKind::ArtDmx { universe, .. } => config
            .universes
            .as_ref()
            .map(|universes| universes.contains(&universe))
            .unwrap_or(true),
        PacketKind::ArtSync | PacketKind::ArtPoll | PacketKind::ArtPollReply => true,
        PacketKind::OtherArtNet { .. } | PacketKind::NonArtNet => {
            config.unknown_policy == UnknownPolicy::Forward
        }
    }
}

fn send_packet(socket: &UdpSocket, packet: &[u8], stats: &Stats) -> io::Result<()> {
    socket.send(packet)?;
    stats.output_packets.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn wait_until(deadline: Instant) {
    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }

        let remaining = deadline - now;
        if remaining > SHORT_SLEEP_WINDOW {
            thread::sleep(remaining - SHORT_SLEEP_WINDOW);
        } else if remaining > SPIN_WINDOW {
            thread::yield_now();
        } else {
            std::hint::spin_loop();
        }
    }
}

fn wait_for_gap(min_gap: Duration, last_send_at: &Option<Instant>) {
    if min_gap.is_zero() {
        return;
    }

    if let Some(last_send_at) = last_send_at {
        wait_until(*last_send_at + min_gap);
    }
}

fn frame_interval(fps: f64) -> Duration {
    Duration::from_secs_f64(1.0 / fps)
}

fn smooth_interval(previous: Duration, observed: Duration) -> Duration {
    if observed < Duration::from_millis(2) || observed > Duration::from_secs(1) {
        return previous;
    }

    previous.mul_f64(0.8) + observed.mul_f64(0.2)
}

fn duration_to_milli_hz(duration: Duration) -> u64 {
    if duration.is_zero() {
        return 0;
    }

    (1000.0 / duration.as_secs_f64()).round() as u64
}

fn spawn_stats_thread(stats: Arc<Stats>) {
    thread::spawn(move || {
        let mut last_input = 0;
        let mut last_output = 0;
        let mut last_dmx = 0;
        let mut last_sync = 0;

        loop {
            thread::sleep(Duration::from_secs(1));

            let input = stats.input_packets.load(Ordering::Relaxed);
            let output = stats.output_packets.load(Ordering::Relaxed);
            let dmx = stats.input_dmx.load(Ordering::Relaxed);
            let sync = stats.input_sync.load(Ordering::Relaxed);
            let dropped = stats.dropped_packets.load(Ordering::Relaxed);
            let late = stats.late_frames.load(Ordering::Relaxed);
            let queued = stats.queued_packets.load(Ordering::Relaxed);
            let milli_hz = stats.frame_rate_milli_hz.load(Ordering::Relaxed);

            println!(
                "stats in_pps={} out_pps={} artdmx_pps={} artsync_pps={} dropped={} late_frames={} queue_depth={} est_fps={:.2}",
                input - last_input,
                output - last_output,
                dmx - last_dmx,
                sync - last_sync,
                dropped,
                late,
                queued,
                milli_hz as f64 / 1000.0
            );

            last_input = input;
            last_output = output;
            last_dmx = dmx;
            last_sync = sync;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_interval_uses_fractional_fps() {
        let interval = frame_interval(60.0);
        assert!((interval.as_secs_f64() - 0.016666).abs() < 0.0001);
    }

    #[test]
    fn smooth_interval_filters_unreasonable_observations() {
        let previous = Duration::from_millis(25);
        assert_eq!(
            smooth_interval(previous, Duration::from_micros(100)),
            previous
        );
        assert_eq!(smooth_interval(previous, Duration::from_secs(2)), previous);
    }
}
