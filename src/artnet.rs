pub const ARTNET_ID: &[u8; 8] = b"Art-Net\0";

pub const OP_POLL: u16 = 0x2000;
pub const OP_POLL_REPLY: u16 = 0x2100;
pub const OP_DMX: u16 = 0x5000;
pub const OP_SYNC: u16 = 0x5200;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PacketKind {
    ArtDmx { universe: u16, length: u16 },
    ArtSync,
    ArtPoll,
    ArtPollReply,
    OtherArtNet { opcode: u16 },
    NonArtNet,
}

pub fn classify(packet: &[u8]) -> PacketKind {
    if packet.len() < 10 || &packet[..8] != ARTNET_ID {
        return PacketKind::NonArtNet;
    }

    let opcode = u16::from_le_bytes([packet[8], packet[9]]);
    match opcode {
        OP_DMX => parse_dmx(packet).unwrap_or(PacketKind::OtherArtNet { opcode }),
        OP_SYNC => PacketKind::ArtSync,
        OP_POLL => PacketKind::ArtPoll,
        OP_POLL_REPLY => PacketKind::ArtPollReply,
        _ => PacketKind::OtherArtNet { opcode },
    }
}

fn parse_dmx(packet: &[u8]) -> Option<PacketKind> {
    if packet.len() < 18 {
        return None;
    }

    let universe = u16::from(packet[14]) | (u16::from(packet[15]) << 8);
    let length = u16::from_be_bytes([packet[16], packet[17]]);
    if packet.len() < 18 + usize::from(length) {
        return None;
    }

    Some(PacketKind::ArtDmx { universe, length })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_art_dmx_and_preserves_port_address() {
        let mut packet = vec![0u8; 18 + 3];
        packet[..8].copy_from_slice(ARTNET_ID);
        packet[8..10].copy_from_slice(&OP_DMX.to_le_bytes());
        packet[14] = 0x34;
        packet[15] = 0x12;
        packet[16..18].copy_from_slice(&3u16.to_be_bytes());

        assert_eq!(
            classify(&packet),
            PacketKind::ArtDmx {
                universe: 0x1234,
                length: 3
            }
        );
    }

    #[test]
    fn rejects_truncated_art_dmx_as_other_artnet() {
        let mut packet = vec![0u8; 19];
        packet[..8].copy_from_slice(ARTNET_ID);
        packet[8..10].copy_from_slice(&OP_DMX.to_le_bytes());
        packet[16..18].copy_from_slice(&2u16.to_be_bytes());

        assert_eq!(
            classify(&packet),
            PacketKind::OtherArtNet { opcode: OP_DMX }
        );
    }

    #[test]
    fn classifies_sync() {
        let mut packet = vec![0u8; 14];
        packet[..8].copy_from_slice(ARTNET_ID);
        packet[8..10].copy_from_slice(&OP_SYNC.to_le_bytes());

        assert_eq!(classify(&packet), PacketKind::ArtSync);
    }
}
