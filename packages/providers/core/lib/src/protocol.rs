use std::sync::Arc;

use bytes::BytesMut;
use tokio::sync::Mutex;
use tokio::{io, net::UnixStream};

use crate::{messages::MessageSlice, Message};

#[repr(u8)]
#[derive(Debug, PartialEq, Clone)]
pub enum Opcode {
    Initialize = 0x1,
    Destroy = 0x2,
    GetValue = 0x3,
    Health = 0x4,
}

impl Opcode {
    fn as_byte(&self) -> u8 {
        log::debug!("Converting opcode {:?} to opcode byte", self);
        let byte = self.clone() as u8;

        log::debug!(
            "TEMP self:{:?} ---- getvalue:{:?} ---- static0x3:{:?} ---- byte:{:?}",
            self,
            Opcode::GetValue,
            0x3,
            byte
        );

        byte
    }

    fn from_byte(byte: u8) -> Result<Opcode, ProtocolError> {
        log::debug!("Converting opcode byte {} to opcode", byte);
        // TODO: has to be a better way, also see similar conversions to improve in LSP, lexer and parser
        match byte {
            0x1 => Ok(Opcode::Initialize),
            0x2 => Ok(Opcode::Destroy),
            0x3 => Ok(Opcode::GetValue),
            0x4 => Ok(Opcode::Health),
            _ => Err(ProtocolError::InvalidOpcode),
        }
    }
}

#[derive(Debug)]
pub struct MessageSerializer {}

type HeaderBytes = Vec<u8>;
type Payload = Vec<u8>;

#[derive(Debug)]
pub struct Header {
    pub version: u8,
    pub opcode: Opcode,
    pub checksum: u16,
    pub payload_length: u32,
}

impl Header {
    fn to_bytes(&self) -> HeaderBytes {
        let opcode = self.opcode.as_byte();
        let mut bytes = vec![self.version, opcode];

        let checksum_bytes = self.checksum.to_be_bytes();
        let payload_length_bytes = self.payload_length.to_be_bytes();

        log::debug!(
            "HeaderBytes to Header, found payload length {:?} from {:?}",
            payload_length_bytes,
            self.payload_length
        );

        bytes = bytes
            .iter()
            .chain(checksum_bytes.iter())
            .chain(payload_length_bytes.iter())
            .copied()
            .collect();

        bytes
    }

    fn from_message(message: MessageSlice) -> Result<Header, ProtocolError> {
        let version = *message.first().ok_or(ProtocolError::InvalidHeader)?;
        log::debug!("Header from message, version:{:?}", version);

        let opcode = *message.get(1).ok_or(ProtocolError::InvalidHeader)?;
        log::debug!("Header from message, opcode:{:?}", opcode);

        let checksum: [u8; 2] = message
            .get(2..=3)
            .ok_or(ProtocolError::InvalidHeader)?
            .try_into()
            .or(Err(ProtocolError::InvalidHeader))?;
        log::debug!("Header from message, checksum:{:?}", checksum);

        let payload_length: [u8; 4] = message
            .get(4..=7)
            .ok_or(ProtocolError::InvalidHeader)?
            .try_into()
            .or(Err(ProtocolError::InvalidHeader))?;
        log::debug!("Header from message, payload_length:{:?}", payload_length);

        Ok(Header {
            version,
            opcode: Opcode::from_byte(opcode)?,
            checksum: u16::from_be_bytes(checksum),
            payload_length: u32::from_be_bytes(payload_length),
        })
    }
}

impl MessageSerializer {
    pub fn serialize(&self, payload: Payload) -> Result<Message, ProtocolError> {
        log::debug!("Serializing payload {:?}", payload);

        let header = MessageSerializer::generate_header(&payload)?;

        Ok(header.iter().chain(payload.iter()).copied().collect())
    }

    fn generate_header(payload: &Payload) -> Result<Vec<u8>, ProtocolError> {
        let mut header = Header {
            version: 0x0, // TODO: get from git tags at compile time??
            opcode: Opcode::GetValue,
            checksum: 0x0,
            payload_length: payload.len() as u32, // TODO: try from
        };

        let header_bytes = header.to_bytes();

        header.checksum = MessageSerializer::generate_checksum(&header_bytes, payload)?;

        Ok(header.to_bytes())
    }

    fn generate_checksum(header_bytes: &Vec<u8>, payload: &Payload) -> Result<u16, ProtocolError> {
        let mut res: u16 = 0;

        let all_bytes: Vec<u8> = header_bytes.iter().chain(payload.iter()).copied().collect();

        for chunk in all_bytes.chunks(2) {
            let pad_count = 2 - chunk.len();

            let mut padding = vec![0; pad_count];
            padding.fill(0);

            let word: Vec<_> = chunk.iter().chain(padding.iter()).copied().collect();
            let word: [u8; 2] = word
                .get(0..=1)
                .ok_or(ProtocolError::UngeneratableChecksum)?
                .try_into()
                .or(Err(ProtocolError::UngeneratableChecksum))?;
            let word = u16::from_be_bytes(word);

            res = res.wrapping_add(word);
        }

        Ok(res)
    }
}

#[derive(Debug)]
pub enum ProtocolError {
    ErrorDetected,
    InvalidHeader,
    InvalidPayload,
    UnreadableStream,
    InvalidOpcode,
    UngeneratableChecksum,
}

pub struct MessageDeserializer {}

pub struct DeserializedMessage {
    pub header: Header,
    pub payload: Payload,
}

impl MessageDeserializer {
    // TODO: use a struct to return instead of tuple
    pub fn deserialize(message: MessageSlice) -> Result<DeserializedMessage, ProtocolError> {
        log::debug!("Deserialize on message called: {:?}", message);

        let header = Header::from_message(message)?;
        let max_payload_index: usize = (8 + header.payload_length)
            .try_into()
            .or(Err(ProtocolError::UnreadableStream))?;

        let payload = message.get(8..max_payload_index);
        log::debug!("TEMP Payload check {:?}", payload);

        let payload: Vec<u8> = payload.ok_or(ProtocolError::InvalidPayload)?.to_vec();

        log::debug!(
            "Deserialize found a payload of len {}: {:?} between indexes 7..{}",
            payload.len(),
            payload,
            max_payload_index
        );

        // TODO: message may have left over bytes from another/the next message.
        // We need to handle this.

        Ok(DeserializedMessage { header, payload })
    }
}

pub struct MessageStreamReader {}

impl MessageStreamReader {
    pub async fn read_message(stream: &Arc<Mutex<UnixStream>>) -> Result<Message, ProtocolError> {
        let mut buf = BytesMut::with_capacity(1024);

        loop {
            let stream = stream.lock().await;

            let n = stream.try_read_buf(&mut buf);

            log::debug!("Read {:?}", n);

            match n {
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if buf.is_empty() {
                        continue;
                    } else {
                        break;
                    }
                }
                Err(_) => {
                    return Err(ProtocolError::UnreadableStream);
                }
                Ok(n) => {
                    if n == 0 {
                        break;
                    }
                }
            }
        }

        Ok(buf.into())
    }
}
