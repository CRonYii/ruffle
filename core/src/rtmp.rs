use flash_lso::amf0::read::AMF0Decoder;
use flash_lso::amf3::read::AMF3Decoder;
use flash_lso::types::{Attribute, Element, ObjectId, Value as AmfValue};
use rand::{TryRngCore, rngs::OsRng};
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;

const HANDSHAKE_SIZE: usize = 1536;
const DEFAULT_CHUNK_SIZE: usize = 128;
const MAX_CHUNK_SIZE: usize = 16 * 1024 * 1024;
const MAX_MESSAGE_SIZE: usize = 32 * 1024 * 1024;
const MAX_INFLIGHT_SIZE: usize = 64 * 1024 * 1024;
const MAX_SAFE_TRANSACTION_ID: u64 = (1_u64 << 53) - 1;

fn response_transaction_id(value: f64) -> Option<u64> {
    if !value.is_finite()
        || value < 2.0
        || value > MAX_SAFE_TRANSACTION_ID as f64
        || value.fract() != 0.0
    {
        return None;
    }
    let transaction_id = value as u64;
    (transaction_id as f64 == value).then_some(transaction_id)
}

#[derive(Debug)]
pub enum RtmpEvent {
    Connected,
    Rejected,
    Closed,
    Result {
        transaction_id: u64,
        value: AmfValue,
    },
    Error {
        transaction_id: u64,
        value: AmfValue,
    },
    Invoke {
        method: String,
        transaction_id: f64,
        arguments: Vec<AmfValue>,
    },
    SharedObject(RtmpSharedObject),
    ProtocolError(String),
}

#[derive(Debug)]
pub struct RtmpSharedObject {
    pub name: String,
    pub version: u32,
    pub flags: [u8; 8],
    pub events: Vec<RtmpSharedObjectEvent>,
}

#[derive(Debug)]
pub enum RtmpSharedObjectEvent {
    UseSuccess,
    Clear,
    Change {
        name: String,
        value: AmfValue,
    },
    Remove {
        name: String,
    },
    SendMessage {
        method: String,
        arguments: Vec<AmfValue>,
    },
    Raw {
        event_type: u8,
        data: Vec<u8>,
    },
}

#[derive(Debug, Clone)]
struct ChunkState {
    timestamp: u32,
    timestamp_delta: u32,
    message_length: usize,
    message_type: u8,
    message_stream_id: u32,
    extended: bool,
    data: Vec<u8>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum Phase {
    Disconnected,
    Handshake,
    Command,
    Connected,
    Failed,
}

#[derive(Debug)]
pub struct RtmpConnection {
    uri: String,
    host: String,
    port: u16,
    app: String,
    connect_arguments: Vec<AmfValue>,
    phase: Phase,
    input: Vec<u8>,
    inbound_chunk_size: usize,
    chunks: BTreeMap<u32, ChunkState>,
    events: VecDeque<RtmpEvent>,
    received_bytes: u32,
    acknowledged_bytes: u32,
    acknowledgement_window: Option<u32>,
}

impl RtmpConnection {
    pub fn new(uri: String, connect_arguments: Vec<AmfValue>) -> Result<Self, String> {
        let parsed = Url::parse(&uri).map_err(|error| format!("Invalid RTMP URL: {error}"))?;
        if parsed.scheme() != "rtmp" {
            return Err("Only plain RTMP is supported".to_string());
        }
        let host = parsed
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or_else(|| "RTMP URL has no host".to_string())?
            .to_string();
        let port = parsed.port().unwrap_or(1935);
        let app = parsed.path().trim_start_matches('/').to_string();
        if app.is_empty() {
            return Err("RTMP URL has no application path".to_string());
        }

        Ok(Self {
            uri,
            host,
            port,
            app,
            connect_arguments,
            phase: Phase::Disconnected,
            input: Vec::new(),
            inbound_chunk_size: DEFAULT_CHUNK_SIZE,
            chunks: BTreeMap::new(),
            events: VecDeque::new(),
            received_bytes: 0,
            acknowledged_bytes: 0,
            acknowledgement_window: None,
        })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn is_connected(&self) -> bool {
        self.phase == Phase::Connected
    }

    pub fn socket_connected(&mut self) -> Vec<u8> {
        self.phase = Phase::Handshake;
        let mut output = Vec::with_capacity(1 + HANDSHAKE_SIZE);
        output.push(3);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as u32;
        output.extend_from_slice(&timestamp.to_be_bytes());
        output.extend_from_slice(&0_u32.to_be_bytes());
        let random_start = output.len();
        output.resize(1 + HANDSHAKE_SIZE, 0);
        OsRng {}
            .try_fill_bytes(&mut output[random_start..])
            .expect("operating system randomness should be available");
        output
    }

    pub fn socket_failed(&mut self, message: String) {
        if self.phase != Phase::Failed {
            self.set_failed();
            self.events.push_back(RtmpEvent::ProtocolError(message));
        }
    }

    pub fn socket_closed(&mut self) {
        if self.phase == Phase::Failed {
            return;
        }
        let was_connected = self.phase == Phase::Connected;
        self.set_failed();
        if was_connected {
            self.events.push_back(RtmpEvent::Closed);
        } else {
            self.events.push_back(RtmpEvent::ProtocolError(
                "RTMP socket closed before connecting".to_string(),
            ));
        }
    }

    pub fn receive(&mut self, data: Vec<u8>) -> Result<Vec<Vec<u8>>, String> {
        if self.phase == Phase::Failed {
            return Err("RTMP connection is closed".to_string());
        }
        let inflight = self.input.len()
            + self
                .chunks
                .values()
                .map(|state| state.data.len())
                .sum::<usize>();
        if inflight.saturating_add(data.len()) > MAX_INFLIGHT_SIZE {
            return self.fail("Aggregate in-flight RTMP data exceeds limit".to_string());
        }
        self.received_bytes = self.received_bytes.wrapping_add(data.len() as u32);
        self.input.extend(data);
        let mut outbound = Vec::new();

        if self.phase == Phase::Handshake {
            if self.input.len() < 1 + HANDSHAKE_SIZE * 2 {
                return Ok(outbound);
            }
            if self.input[0] != 3 {
                return self.fail(format!(
                    "Unsupported RTMP handshake version {}",
                    self.input[0]
                ));
            }
            let c2 = self.input[1..1 + HANDSHAKE_SIZE].to_vec();
            self.input.drain(..1 + HANDSHAKE_SIZE * 2);
            self.phase = Phase::Command;

            let mut handshake_and_connect = c2;
            let connect = match self.connect_payload() {
                Ok(connect) => connect,
                Err(error) => return self.fail(error),
            };
            handshake_and_connect.extend(encode_message(&connect, 3, 20, 0, 0, DEFAULT_CHUNK_SIZE));
            outbound.push(handshake_and_connect);
        }

        if matches!(self.phase, Phase::Command | Phase::Connected) {
            loop {
                let buffered = self.input.len();
                match self.next_message()? {
                    Some(message) => self.handle_message(message, &mut outbound)?,
                    None if self.input.len() < buffered => continue,
                    None => break,
                }
            }
            if let Some(window) = self.acknowledgement_window
                && self.received_bytes.wrapping_sub(self.acknowledged_bytes) >= window
            {
                self.acknowledged_bytes = self.received_bytes;
                outbound.push(encode_message(
                    &self.received_bytes.to_be_bytes(),
                    2,
                    3,
                    0,
                    0,
                    DEFAULT_CHUNK_SIZE,
                ));
            }
        }

        Ok(outbound)
    }

    pub fn send_command(
        &self,
        method: &str,
        transaction_id: f64,
        arguments: &AmfValue,
    ) -> Result<Vec<u8>, String> {
        let mut payload = vec![0];
        encode_amf0(&AmfValue::String(method.to_string()), &mut payload)?;
        encode_amf0(&AmfValue::Number(transaction_id), &mut payload)?;
        encode_amf0(&AmfValue::Null, &mut payload)?;
        if let AmfValue::StrictArray(_, values) = arguments {
            for argument in values {
                encode_amf0(argument, &mut payload)?;
            }
        } else {
            encode_amf0(arguments, &mut payload)?;
        }
        Ok(encode_message(&payload, 3, 17, 0, 0, DEFAULT_CHUNK_SIZE))
    }

    pub fn send_shared_object_event(&self, name: &str, event_type: u8) -> Result<Vec<u8>, String> {
        encode_shared_object_message(name, 0, event_type, &[])
    }

    pub fn send_shared_object_message(
        &self,
        name: &str,
        method: &str,
        arguments: &[AmfValue],
    ) -> Result<Vec<u8>, String> {
        let mut encoder = Amf3Encoder::default();
        let mut body = Vec::new();
        encoder.encode(&AmfValue::String(method.to_string()), &mut body)?;
        for argument in arguments {
            encoder.encode(argument, &mut body)?;
        }
        encode_shared_object_message(name, 0, 6, &body)
    }

    pub fn take_events(&mut self) -> Vec<RtmpEvent> {
        self.events.drain(..).collect()
    }

    fn connect_payload(&self) -> Result<Vec<u8>, String> {
        let properties = vec![
            element("app", AmfValue::String(self.app.clone())),
            element("flashVer", AmfValue::String("WIN 32,0,0,465".to_string())),
            element("swfUrl", AmfValue::String(String::new())),
            element("tcUrl", AmfValue::String(self.uri.clone())),
            element("fpad", AmfValue::Bool(false)),
            element("capabilities", AmfValue::Number(239.0)),
            element("audioCodecs", AmfValue::Number(3575.0)),
            element("videoCodecs", AmfValue::Number(252.0)),
            element("videoFunction", AmfValue::Number(1.0)),
            element("pageUrl", AmfValue::String(String::new())),
            element("objectEncoding", AmfValue::Number(3.0)),
        ];
        let mut payload = Vec::new();
        encode_amf0(&AmfValue::String("connect".to_string()), &mut payload)?;
        encode_amf0(&AmfValue::Number(1.0), &mut payload)?;
        encode_amf0(
            &AmfValue::Object(ObjectId::INVALID, properties, None),
            &mut payload,
        )?;
        for argument in &self.connect_arguments {
            encode_amf0(argument, &mut payload)?;
        }
        Ok(payload)
    }

    fn next_message(&mut self) -> Result<Option<RtmpMessage>, String> {
        if self.input.is_empty() {
            return Ok(None);
        }
        let data = &self.input;
        let mut position = 0;
        let first = data[position];
        position += 1;
        let format = first >> 6;
        let mut chunk_stream_id = u32::from(first & 0x3f);
        if chunk_stream_id == 0 {
            if data.len() < position + 1 {
                return Ok(None);
            }
            chunk_stream_id = 64 + u32::from(data[position]);
            position += 1;
        } else if chunk_stream_id == 1 {
            if data.len() < position + 2 {
                return Ok(None);
            }
            chunk_stream_id = 64 + u32::from(data[position]) + 256 * u32::from(data[position + 1]);
            position += 2;
        }

        let previous = self.chunks.get(&chunk_stream_id).cloned();
        if format != 0 && previous.is_none() {
            return self.fail(format!(
                "RTMP chunk format {format} has no prior header for stream {chunk_stream_id}"
            ));
        }
        let continuation = previous
            .as_ref()
            .is_some_and(|state| !state.data.is_empty() && state.data.len() < state.message_length);

        let mut state = match format {
            0 => {
                if data.len() < position + 11 {
                    return Ok(None);
                }
                let raw_timestamp = read_u24(&data[position..position + 3]);
                let message_length = read_u24(&data[position + 3..position + 6]) as usize;
                let message_type = data[position + 6];
                let message_stream_id =
                    u32::from_le_bytes(data[position + 7..position + 11].try_into().unwrap());
                position += 11;
                let extended = raw_timestamp == 0x00ff_ffff;
                let timestamp = if extended {
                    if data.len() < position + 4 {
                        return Ok(None);
                    }
                    let value =
                        u32::from_be_bytes(data[position..position + 4].try_into().unwrap());
                    position += 4;
                    value
                } else {
                    raw_timestamp
                };
                ChunkState {
                    timestamp,
                    timestamp_delta: 0,
                    message_length,
                    message_type,
                    message_stream_id,
                    extended,
                    data: Vec::new(),
                }
            }
            1 => {
                if data.len() < position + 7 {
                    return Ok(None);
                }
                let previous = previous.unwrap();
                let raw_delta = read_u24(&data[position..position + 3]);
                let message_length = read_u24(&data[position + 3..position + 6]) as usize;
                let message_type = data[position + 6];
                position += 7;
                let extended = raw_delta == 0x00ff_ffff;
                let delta = if extended {
                    if data.len() < position + 4 {
                        return Ok(None);
                    }
                    let value =
                        u32::from_be_bytes(data[position..position + 4].try_into().unwrap());
                    position += 4;
                    value
                } else {
                    raw_delta
                };
                ChunkState {
                    timestamp: previous.timestamp.wrapping_add(delta),
                    timestamp_delta: delta,
                    message_length,
                    message_type,
                    message_stream_id: previous.message_stream_id,
                    extended,
                    data: Vec::new(),
                }
            }
            2 => {
                if data.len() < position + 3 {
                    return Ok(None);
                }
                let previous = previous.unwrap();
                let raw_delta = read_u24(&data[position..position + 3]);
                position += 3;
                let extended = raw_delta == 0x00ff_ffff;
                let delta = if extended {
                    if data.len() < position + 4 {
                        return Ok(None);
                    }
                    let value =
                        u32::from_be_bytes(data[position..position + 4].try_into().unwrap());
                    position += 4;
                    value
                } else {
                    raw_delta
                };
                ChunkState {
                    timestamp: previous.timestamp.wrapping_add(delta),
                    timestamp_delta: delta,
                    message_length: previous.message_length,
                    message_type: previous.message_type,
                    message_stream_id: previous.message_stream_id,
                    extended,
                    data: Vec::new(),
                }
            }
            3 => {
                let previous = previous.unwrap();
                let next = if continuation {
                    previous
                } else {
                    ChunkState {
                        timestamp: previous.timestamp.wrapping_add(previous.timestamp_delta),
                        data: Vec::new(),
                        ..previous
                    }
                };
                if next.extended {
                    if data.len() < position + 4 {
                        return Ok(None);
                    }
                    position += 4;
                }
                next
            }
            _ => unreachable!(),
        };

        if state.message_length > MAX_MESSAGE_SIZE {
            return self.fail(format!(
                "RTMP message length {} exceeds limit",
                state.message_length
            ));
        }
        let remaining = match state.message_length.checked_sub(state.data.len()) {
            Some(remaining) => remaining,
            None => return self.fail("RTMP chunk exceeds declared message length".to_string()),
        };
        let take = self.inbound_chunk_size.min(remaining);
        if data.len() < position + take {
            return Ok(None);
        }
        state
            .data
            .extend_from_slice(&data[position..position + take]);
        self.input.drain(..position + take);

        if state.data.len() != state.message_length {
            self.chunks.insert(chunk_stream_id, state);
            return Ok(None);
        }

        let message = RtmpMessage {
            message_type: state.message_type,
            message_stream_id: state.message_stream_id,
            timestamp: state.timestamp,
            payload: std::mem::take(&mut state.data),
        };
        self.chunks.insert(chunk_stream_id, state);
        Ok(Some(message))
    }

    fn handle_message(
        &mut self,
        message: RtmpMessage,
        outbound: &mut Vec<Vec<u8>>,
    ) -> Result<(), String> {
        match message.message_type {
            1 => {
                if message.payload.len() != 4 {
                    return self.fail("Set Chunk Size payload must be 4 bytes".to_string());
                }
                let raw_size = u32::from_be_bytes(message.payload[..4].try_into().unwrap());
                if raw_size & 0x8000_0000 != 0 {
                    return self.fail("Set Chunk Size reserved bit is set".to_string());
                }
                let size = raw_size as usize;
                if !(1..=MAX_CHUNK_SIZE).contains(&size) {
                    return self.fail(format!("Invalid RTMP chunk size {size}"));
                }
                self.inbound_chunk_size = size;
            }
            2 => {
                if message.payload.len() != 4 {
                    return self.fail("Abort payload must be 4 bytes".to_string());
                }
                let stream = u32::from_be_bytes(message.payload[..4].try_into().unwrap());
                if let Some(state) = self.chunks.get_mut(&stream) {
                    state.data.clear();
                }
            }
            3 => {
                if message.payload.len() != 4 {
                    return self.fail("Acknowledgement payload must be 4 bytes".to_string());
                }
            }
            4 => {
                if message.payload.len() < 2 {
                    return self.fail("User Control payload is truncated".to_string());
                }
                let event_type = u16::from_be_bytes(message.payload[..2].try_into().unwrap());
                let expected_length = match event_type {
                    0 | 1 | 2 | 4 | 6 | 7 => Some(6),
                    3 => Some(10),
                    _ => None,
                };
                if let Some(expected_length) = expected_length
                    && message.payload.len() != expected_length
                {
                    return self.fail(format!(
                        "User Control event {event_type} payload must be {expected_length} bytes"
                    ));
                }
                if event_type == 6 {
                    let mut pong = Vec::with_capacity(6);
                    pong.extend_from_slice(&7_u16.to_be_bytes());
                    pong.extend_from_slice(&message.payload[2..6]);
                    outbound.push(encode_message(&pong, 2, 4, 0, 0, DEFAULT_CHUNK_SIZE));
                }
            }
            5 => {
                if message.payload.len() != 4 {
                    return self
                        .fail("Window Acknowledgement Size payload must be 4 bytes".to_string());
                }
                let window = u32::from_be_bytes(message.payload[..4].try_into().unwrap());
                if window == 0 {
                    return self.fail("Window Acknowledgement Size must be nonzero".to_string());
                }
                self.acknowledgement_window = Some(window);
            }
            6 => {
                if message.payload.len() != 5 {
                    return self.fail("Set Peer Bandwidth payload must be 5 bytes".to_string());
                }
                let window = u32::from_be_bytes(message.payload[..4].try_into().unwrap());
                if window == 0 || message.payload[4] > 2 {
                    return self.fail("Invalid Set Peer Bandwidth payload".to_string());
                }
                self.acknowledgement_window = Some(window);
            }
            16 | 19 => match decode_shared_object(&message.payload, message.message_type) {
                Ok(shared_object) => self
                    .events
                    .push_back(RtmpEvent::SharedObject(shared_object)),
                Err(error) => return self.fail(error),
            },
            17 | 20 => {
                let body = if message.message_type == 17 {
                    if message.payload.first() != Some(&0) {
                        return self.fail("Unsupported AMF3 command encoding".to_string());
                    }
                    &message.payload[1..]
                } else {
                    &message.payload
                };
                let values = match decode_amf0_sequence(body) {
                    Ok(values) => values,
                    Err(error) => return self.fail(error),
                };
                let Some(AmfValue::String(method)) = values.first() else {
                    return self.fail("RTMP command has no string method name".to_string());
                };
                let transaction_id = match values.get(1) {
                    Some(AmfValue::Number(value)) if value.is_finite() && *value >= 0.0 => *value,
                    _ => return self.fail("RTMP command has invalid transaction ID".to_string()),
                };
                if method == "_result" && transaction_id == 1.0 {
                    self.phase = Phase::Connected;
                    self.events.push_back(RtmpEvent::Connected);
                } else if (method == "_error" || method == "onStatus") && transaction_id == 1.0 {
                    self.set_failed();
                    self.events.push_back(RtmpEvent::Rejected);
                } else if (method == "_result" || method == "_error") && transaction_id >= 2.0 {
                    let Some(transaction_id) = response_transaction_id(transaction_id) else {
                        return self.fail("RTMP response has invalid transaction ID".to_string());
                    };
                    let value = values.get(3).cloned().unwrap_or(AmfValue::Undefined);
                    if method == "_result" {
                        self.events.push_back(RtmpEvent::Result {
                            transaction_id,
                            value,
                        });
                    } else {
                        self.events.push_back(RtmpEvent::Error {
                            transaction_id,
                            value,
                        });
                    }
                } else if !method.starts_with('_') {
                    self.events.push_back(RtmpEvent::Invoke {
                        method: method.clone(),
                        transaction_id,
                        arguments: values.into_iter().skip(3).collect(),
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn set_failed(&mut self) {
        self.phase = Phase::Failed;
        self.input.clear();
        self.chunks.clear();
    }

    fn fail<T>(&mut self, message: String) -> Result<T, String> {
        if self.phase != Phase::Failed {
            self.set_failed();
            self.events
                .push_back(RtmpEvent::ProtocolError(message.clone()));
        }
        Err(message)
    }
}

#[derive(Debug)]
struct RtmpMessage {
    message_type: u8,
    #[allow(dead_code)]
    message_stream_id: u32,
    #[allow(dead_code)]
    timestamp: u32,
    payload: Vec<u8>,
}

fn decode_shared_object(payload: &[u8], message_type: u8) -> Result<RtmpSharedObject, String> {
    let mut position = 0;
    if message_type == 16 {
        if payload.first() != Some(&0) {
            return Err("Unsupported AMF3 SharedObject encoding".to_string());
        }
        position = 1;
    }
    if payload.len().saturating_sub(position) < 14 {
        return Err("SharedObject envelope is truncated".to_string());
    }
    let name_length =
        u16::from_be_bytes(payload[position..position + 2].try_into().unwrap()) as usize;
    position += 2;
    if payload.len().saturating_sub(position) < name_length + 12 {
        return Err("SharedObject name or envelope is truncated".to_string());
    }
    let name = std::str::from_utf8(&payload[position..position + name_length])
        .map_err(|_| "SharedObject name is not UTF-8".to_string())?
        .to_string();
    position += name_length;
    let version = u32::from_be_bytes(payload[position..position + 4].try_into().unwrap());
    position += 4;
    let flags = payload[position..position + 8].try_into().unwrap();
    position += 8;
    let mut events = Vec::new();

    while position < payload.len() {
        if payload.len() - position < 5 {
            return Err("SharedObject event header is truncated".to_string());
        }
        let event_type = payload[position];
        let event_length =
            u32::from_be_bytes(payload[position + 1..position + 5].try_into().unwrap()) as usize;
        position += 5;
        if event_length > payload.len() - position {
            return Err(format!(
                "SharedObject event {event_type} length {event_length} exceeds remaining payload"
            ));
        }
        let body = &payload[position..position + event_length];
        position += event_length;
        let event = match event_type {
            11 => {
                if !body.is_empty() {
                    return Err("SharedObject use-success event must be empty".to_string());
                }
                RtmpSharedObjectEvent::UseSuccess
            }
            8 => {
                if !body.is_empty() {
                    return Err("SharedObject clear event must be empty".to_string());
                }
                RtmpSharedObjectEvent::Clear
            }
            4 => {
                if body.len() < 2 {
                    return Err("SharedObject change event is truncated".to_string());
                }
                let name_length = u16::from_be_bytes(body[..2].try_into().unwrap()) as usize;
                if body.len() < 2 + name_length {
                    return Err("SharedObject change name is truncated".to_string());
                }
                let name = std::str::from_utf8(&body[2..2 + name_length])
                    .map_err(|_| "SharedObject change name is not UTF-8".to_string())?
                    .to_string();
                let values = if message_type == 16 {
                    decode_amf3_sequence(&body[2 + name_length..])?
                } else {
                    decode_amf0_sequence(&body[2 + name_length..])?
                };
                let [value] = values.as_slice() else {
                    return Err("SharedObject change must contain one value".to_string());
                };
                RtmpSharedObjectEvent::Change {
                    name,
                    value: value.clone(),
                }
            }
            9 => {
                if body.len() < 2 {
                    return Err("SharedObject remove event is truncated".to_string());
                }
                let name_length = u16::from_be_bytes(body[..2].try_into().unwrap()) as usize;
                if body.len() != 2 + name_length {
                    return Err("SharedObject remove name has invalid length".to_string());
                }
                let name = std::str::from_utf8(&body[2..])
                    .map_err(|_| "SharedObject remove name is not UTF-8".to_string())?
                    .to_string();
                RtmpSharedObjectEvent::Remove { name }
            }
            6 => {
                let values = if message_type == 16 {
                    decode_amf3_sequence(body)?
                } else {
                    decode_amf0_sequence(body)?
                };
                let Some(AmfValue::String(method)) = values.first() else {
                    return Err("SharedObject send-message has no string method name".to_string());
                };
                RtmpSharedObjectEvent::SendMessage {
                    method: method.clone(),
                    arguments: values.into_iter().skip(1).collect(),
                }
            }
            _ => RtmpSharedObjectEvent::Raw {
                event_type,
                data: body.to_vec(),
            },
        };
        events.push(event);
    }

    Ok(RtmpSharedObject {
        name,
        version,
        flags,
        events,
    })
}

fn encode_shared_object_message(
    name: &str,
    version: u32,
    event_type: u8,
    event_body: &[u8],
) -> Result<Vec<u8>, String> {
    let name = name.as_bytes();
    if name.len() > u16::MAX as usize {
        return Err("SharedObject name is too long".to_string());
    }
    let event_length = u32::try_from(event_body.len())
        .map_err(|_| "SharedObject event is too large".to_string())?;
    let mut payload = Vec::with_capacity(20 + name.len() + event_body.len());
    payload.push(0);
    payload.extend_from_slice(&(name.len() as u16).to_be_bytes());
    payload.extend_from_slice(name);
    payload.extend_from_slice(&version.to_be_bytes());
    payload.extend_from_slice(&[0; 8]);
    payload.push(event_type);
    payload.extend_from_slice(&event_length.to_be_bytes());
    payload.extend_from_slice(event_body);
    Ok(encode_message(&payload, 3, 16, 0, 0, DEFAULT_CHUNK_SIZE))
}

#[derive(Default)]
struct Amf3Encoder {
    strings: Vec<String>,
}

impl Amf3Encoder {
    fn encode(&mut self, value: &AmfValue, output: &mut Vec<u8>) -> Result<(), String> {
        match value {
            AmfValue::Undefined | AmfValue::Unsupported => output.push(0),
            AmfValue::Null => output.push(1),
            AmfValue::Bool(false) => output.push(2),
            AmfValue::Bool(true) => output.push(3),
            AmfValue::Integer(value) if (-268_435_456..=268_435_455).contains(value) => {
                output.push(4);
                write_u29(output, *value as u32 & 0x1fff_ffff);
            }
            AmfValue::Integer(value) => {
                output.push(5);
                output.extend_from_slice(&(*value as f64).to_be_bytes());
            }
            AmfValue::Number(value) => {
                output.push(5);
                output.extend_from_slice(&value.to_be_bytes());
            }
            AmfValue::String(value) => {
                output.push(6);
                self.write_string(value, output)?;
            }
            AmfValue::Date(value, _) => {
                output.push(8);
                write_u29(output, 1);
                output.extend_from_slice(&value.to_be_bytes());
            }
            AmfValue::XML(value, is_string) => {
                output.push(if *is_string { 11 } else { 7 });
                write_inline_bytes(value.as_bytes(), output)?;
            }
            AmfValue::ECMAArray(_, dense, elements, _) => {
                output.push(9);
                write_u29_length(dense.len(), output)?;
                for element in elements {
                    self.write_string(element.name(), output)?;
                    self.encode(element.value(), output)?;
                }
                self.write_string("", output)?;
                for value in dense {
                    self.encode(value, output)?;
                }
            }
            AmfValue::StrictArray(_, values) => {
                output.push(9);
                write_u29_length(values.len(), output)?;
                self.write_string("", output)?;
                for value in values {
                    self.encode(value, output)?;
                }
            }
            AmfValue::Object(_, elements, class) => {
                output.push(10);
                self.encode_object(elements, class.as_ref(), output)?;
            }
            AmfValue::ByteArray(bytes) => {
                output.push(12);
                write_inline_bytes(bytes, output)?;
            }
            AmfValue::VectorInt(values, fixed) => {
                output.push(13);
                write_u29_length(values.len(), output)?;
                output.push(u8::from(*fixed));
                for value in values {
                    output.extend_from_slice(&value.to_be_bytes());
                }
            }
            AmfValue::VectorUInt(values, fixed) => {
                output.push(14);
                write_u29_length(values.len(), output)?;
                output.push(u8::from(*fixed));
                for value in values {
                    output.extend_from_slice(&value.to_be_bytes());
                }
            }
            AmfValue::VectorDouble(values, fixed) => {
                output.push(15);
                write_u29_length(values.len(), output)?;
                output.push(u8::from(*fixed));
                for value in values {
                    output.extend_from_slice(&value.to_be_bytes());
                }
            }
            AmfValue::VectorObject(_, values, type_name, fixed) => {
                output.push(16);
                write_u29_length(values.len(), output)?;
                output.push(u8::from(*fixed));
                self.write_string(type_name, output)?;
                for value in values {
                    self.encode(value, output)?;
                }
            }
            AmfValue::Dictionary(_, entries, weak_keys) => {
                output.push(17);
                write_u29_length(entries.len(), output)?;
                output.push(u8::from(*weak_keys));
                for (key, value) in entries {
                    self.encode(key, output)?;
                    self.encode(value, output)?;
                }
            }
            AmfValue::Custom(elements, dynamic, class) => {
                output.push(10);
                if class
                    .as_ref()
                    .is_some_and(|class| class.attributes.contains(Attribute::External))
                {
                    return Err("Externalizable AMF3 objects are unsupported".to_string());
                }
                let mut all_elements = elements.clone();
                all_elements.extend(dynamic.iter().cloned());
                self.encode_object(&all_elements, class.as_ref(), output)?;
            }
            AmfValue::AMF3(value) => self.encode(value, output)?,
            AmfValue::Reference(_) | AmfValue::Amf3ObjectReference(_) => {
                return Err("Outbound AMF object references are unsupported".to_string());
            }
        }
        Ok(())
    }

    fn encode_object(
        &mut self,
        elements: &[Element],
        class: Option<&flash_lso::types::ClassDefinition>,
        output: &mut Vec<u8>,
    ) -> Result<(), String> {
        let sealed = class.map_or(&[][..], |class| class.static_properties.as_slice());
        let dynamic = class.is_none_or(|class| class.attributes.contains(Attribute::Dynamic));
        let traits = (u32::try_from(sealed.len())
            .map_err(|_| "Too many AMF3 sealed properties".to_string())?
            << 4)
            | 3
            | if dynamic { 8 } else { 0 };
        write_u29(output, traits);
        self.write_string(class.map_or("", |class| class.name.as_str()), output)?;
        for name in sealed {
            self.write_string(name, output)?;
        }
        for name in sealed {
            let value = elements
                .iter()
                .find(|element| element.name() == name)
                .map(Element::value)
                .unwrap_or(&AmfValue::Undefined);
            self.encode(value, output)?;
        }
        if dynamic {
            for element in elements
                .iter()
                .filter(|element| !sealed.iter().any(|name| name == element.name()))
            {
                self.write_string(element.name(), output)?;
                self.encode(element.value(), output)?;
            }
            self.write_string("", output)?;
        }
        Ok(())
    }

    fn write_string(&mut self, value: &str, output: &mut Vec<u8>) -> Result<(), String> {
        if value.is_empty() {
            write_u29(output, 1);
            return Ok(());
        }
        if let Some(index) = self.strings.iter().position(|string| string == value) {
            write_u29(output, index as u32);
        } else {
            write_inline_bytes(value.as_bytes(), output)?;
            self.strings.push(value.to_string());
        }
        Ok(())
    }
}

fn write_u29(output: &mut Vec<u8>, value: u32) {
    let value = value & 0x1fff_ffff;
    if value < 0x80 {
        output.push(value as u8);
    } else if value < 0x4000 {
        output.push(((value >> 7) | 0x80) as u8);
        output.push((value & 0x7f) as u8);
    } else if value < 0x20_0000 {
        output.push(((value >> 14) | 0x80) as u8);
        output.push((((value >> 7) & 0x7f) | 0x80) as u8);
        output.push((value & 0x7f) as u8);
    } else {
        output.push(((value >> 22) | 0x80) as u8);
        output.push((((value >> 15) & 0x7f) | 0x80) as u8);
        output.push((((value >> 8) & 0x7f) | 0x80) as u8);
        output.push((value & 0xff) as u8);
    }
}

fn write_u29_length(length: usize, output: &mut Vec<u8>) -> Result<(), String> {
    let length = u32::try_from(length).map_err(|_| "AMF3 collection is too large".to_string())?;
    if length > 0x0fff_ffff {
        return Err("AMF3 collection is too large".to_string());
    }
    write_u29(output, (length << 1) | 1);
    Ok(())
}

fn write_inline_bytes(bytes: &[u8], output: &mut Vec<u8>) -> Result<(), String> {
    write_u29_length(bytes.len(), output)?;
    output.extend_from_slice(bytes);
    Ok(())
}

fn element(name: &str, value: AmfValue) -> Element {
    Element::new(name, Rc::new(value))
}

fn read_u24(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([0, bytes[0], bytes[1], bytes[2]])
}

fn write_u24(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_be_bytes()[1..]);
}

fn encode_message(
    payload: &[u8],
    chunk_stream_id: u8,
    message_type: u8,
    message_stream_id: u32,
    timestamp: u32,
    chunk_size: usize,
) -> Vec<u8> {
    let mut output = Vec::new();
    output.push(chunk_stream_id);
    write_u24(&mut output, timestamp.min(0x00ff_ffff));
    write_u24(&mut output, payload.len() as u32);
    output.push(message_type);
    output.extend_from_slice(&message_stream_id.to_le_bytes());
    if timestamp >= 0x00ff_ffff {
        output.extend_from_slice(&timestamp.to_be_bytes());
    }
    for (index, chunk) in payload.chunks(chunk_size).enumerate() {
        if index != 0 {
            output.push(0xc0 | chunk_stream_id);
            if timestamp >= 0x00ff_ffff {
                output.extend_from_slice(&timestamp.to_be_bytes());
            }
        }
        output.extend_from_slice(chunk);
    }
    output
}

fn encode_amf0(value: &AmfValue, output: &mut Vec<u8>) -> Result<(), String> {
    match value {
        AmfValue::Number(value) => {
            output.push(0);
            output.extend_from_slice(&value.to_be_bytes());
        }
        AmfValue::Bool(value) => {
            output.extend_from_slice(&[1, u8::from(*value)]);
        }
        AmfValue::String(value) => {
            let bytes = value.as_bytes();
            if bytes.len() > u16::MAX as usize {
                output.push(12);
                output.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            } else {
                output.push(2);
                output.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            }
            output.extend_from_slice(bytes);
        }
        AmfValue::Object(_, elements, _) => {
            output.push(3);
            encode_object_elements(elements, output)?;
        }
        AmfValue::Null => output.push(5),
        AmfValue::Undefined | AmfValue::Unsupported => output.push(6),
        AmfValue::ECMAArray(_, dense, elements, length) => {
            output.push(8);
            output.extend_from_slice(&length.to_be_bytes());
            for (index, value) in dense.iter().enumerate() {
                encode_property(&index.to_string(), value, output)?;
            }
            encode_object_elements(elements, output)?;
        }
        AmfValue::StrictArray(_, values) => {
            output.push(10);
            output.extend_from_slice(&(values.len() as u32).to_be_bytes());
            for value in values {
                encode_amf0(value, output)?;
            }
        }
        AmfValue::Date(value, timezone) => {
            output.push(11);
            output.extend_from_slice(&value.to_be_bytes());
            output.extend_from_slice(&timezone.unwrap_or(0).to_be_bytes());
        }
        AmfValue::XML(value, _) => {
            output.push(15);
            output.extend_from_slice(&(value.len() as u32).to_be_bytes());
            output.extend_from_slice(value.as_bytes());
        }
        AmfValue::AMF3(value) => {
            output.push(17);
            Amf3Encoder::default().encode(value, output)?;
        }
        AmfValue::Integer(_)
        | AmfValue::ByteArray(_)
        | AmfValue::VectorInt(_, _)
        | AmfValue::VectorUInt(_, _)
        | AmfValue::VectorDouble(_, _)
        | AmfValue::VectorObject(_, _, _, _)
        | AmfValue::Dictionary(_, _, _)
        | AmfValue::Custom(_, _, _) => {
            output.push(17);
            Amf3Encoder::default().encode(value, output)?;
        }
        AmfValue::Reference(_) | AmfValue::Amf3ObjectReference(_) => {
            return Err("Unsupported outbound AMF reference".to_string());
        }
    }
    Ok(())
}

fn encode_object_elements(elements: &[Element], output: &mut Vec<u8>) -> Result<(), String> {
    for item in elements {
        encode_property(item.name(), item.value(), output)?;
    }
    output.extend_from_slice(&[0, 0, 9]);
    Ok(())
}

fn encode_property(name: &str, value: &AmfValue, output: &mut Vec<u8>) -> Result<(), String> {
    if name.len() > u16::MAX as usize {
        return Err("AMF property name is too long".to_string());
    }
    output.extend_from_slice(&(name.len() as u16).to_be_bytes());
    output.extend_from_slice(name.as_bytes());
    encode_amf0(value, output)
}

fn decode_amf0_sequence(mut input: &[u8]) -> Result<Vec<AmfValue>, String> {
    let mut decoder = AMF0Decoder::default();
    let mut values = Vec::new();
    while !input.is_empty() {
        let before = input.len();
        let (remaining, value) = decoder
            .parse_single_element(input)
            .map_err(|_| "Invalid AMF0 payload".to_string())?;
        if remaining.len() >= before {
            return Err("AMF0 decoder made no progress".to_string());
        }
        values.push((*value).clone());
        input = remaining;
    }
    Ok(values)
}

fn decode_amf3_sequence(mut input: &[u8]) -> Result<Vec<AmfValue>, String> {
    let mut decoder = AMF3Decoder::default();
    let mut values = Vec::new();
    while !input.is_empty() {
        let before = input.len();
        let (remaining, value) = decoder
            .parse_single_element(input)
            .map_err(|_| "Invalid AMF3 payload".to_string())?;
        if remaining.len() >= before {
            return Err("AMF3 decoder made no progress".to_string());
        }
        values.push((*value).clone());
        input = remaining;
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command_payload(method: &str, transaction_id: f64, arguments: &[AmfValue]) -> Vec<u8> {
        let mut payload = Vec::new();
        encode_amf0(&AmfValue::String(method.to_string()), &mut payload).unwrap();
        encode_amf0(&AmfValue::Number(transaction_id), &mut payload).unwrap();
        encode_amf0(&AmfValue::Null, &mut payload).unwrap();
        for argument in arguments {
            encode_amf0(argument, &mut payload).unwrap();
        }
        payload
    }

    #[test]
    fn handshake_is_incremental_and_sends_connect() {
        let mut connection = RtmpConnection::new(
            "rtmp://127.0.0.1:1935/taoofms/s72.example".to_string(),
            vec![AmfValue::String("user".to_string())],
        )
        .unwrap();
        assert_eq!(connection.socket_connected().len(), 1537);

        let mut server_handshake = vec![3];
        server_handshake.extend(vec![7; HANDSHAKE_SIZE]);
        server_handshake.extend(vec![9; HANDSHAKE_SIZE]);
        assert!(
            connection
                .receive(server_handshake[..1000].to_vec())
                .unwrap()
                .is_empty()
        );
        let output = connection
            .receive(server_handshake[1000..].to_vec())
            .unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(&output[0][..HANDSHAKE_SIZE], vec![7; HANDSHAKE_SIZE]);
        assert!(
            output[0][HANDSHAKE_SIZE..]
                .windows(10)
                .any(|bytes| bytes == b"\x02\x00\x07connect")
        );
    }

    #[test]
    fn response_transaction_ids_must_be_exact_safe_integers() {
        assert_eq!(response_transaction_id(2.0), Some(2));
        assert_eq!(
            response_transaction_id(9_007_199_254_740_991.0),
            Some(9_007_199_254_740_991)
        );
        for invalid in [
            -1.0,
            0.0,
            1.0,
            2.5,
            f64::NAN,
            f64::INFINITY,
            9_007_199_254_740_992.0,
        ] {
            assert_eq!(response_transaction_id(invalid), None);
        }
    }

    #[test]
    fn fractional_response_transaction_id_fails_without_result_event() {
        let mut connection = RtmpConnection::new(
            "rtmp://127.0.0.1/taoofms/s72.example".to_string(),
            Vec::new(),
        )
        .unwrap();
        connection.phase = Phase::Connected;
        let payload = command_payload("_result", 2.5, &[AmfValue::Null]);
        let message = encode_message(&payload, 3, 20, 0, 0, DEFAULT_CHUNK_SIZE);

        assert!(connection.receive(message).is_err());
        assert!(matches!(
            connection.take_events().as_slice(),
            [RtmpEvent::ProtocolError(message)] if message.contains("transaction ID")
        ));
    }

    #[test]
    fn fragmented_result_marks_connection_connected() {
        let mut connection = RtmpConnection::new(
            "rtmp://127.0.0.1/taoofms/s72.example".to_string(),
            Vec::new(),
        )
        .unwrap();
        connection.socket_connected();
        let mut server_handshake = vec![3];
        server_handshake.extend(vec![0; HANDSHAKE_SIZE * 2]);
        connection.receive(server_handshake).unwrap();

        let payload = command_payload("_result", 1.0, &[AmfValue::Null]);
        let message = encode_message(&payload, 3, 20, 0, 0, DEFAULT_CHUNK_SIZE);
        for byte in message {
            connection.receive(vec![byte]).unwrap();
        }
        assert!(connection.is_connected());
        assert!(matches!(
            connection.take_events().as_slice(),
            [RtmpEvent::Connected]
        ));
    }

    fn shared_object_payload(
        message_type: u8,
        name: &str,
        version: u32,
        events: &[(u8, &[u8])],
    ) -> Vec<u8> {
        let mut payload = Vec::new();
        if message_type == 16 {
            payload.push(0);
        }
        payload.extend_from_slice(&(name.len() as u16).to_be_bytes());
        payload.extend_from_slice(name.as_bytes());
        payload.extend_from_slice(&version.to_be_bytes());
        payload.extend_from_slice(&[0; 8]);
        for (event_type, body) in events {
            payload.push(*event_type);
            payload.extend_from_slice(&(body.len() as u32).to_be_bytes());
            payload.extend_from_slice(body);
        }
        payload
    }

    #[test]
    fn shared_object_subscription_matches_captured_shape() {
        let connection = RtmpConnection::new(
            "rtmp://127.0.0.1/taoofms/s72.example".to_string(),
            Vec::new(),
        )
        .unwrap();
        let message = connection
            .send_shared_object_event("War_monster_60", 1)
            .unwrap();
        let expected = shared_object_payload(16, "War_monster_60", 0, &[(1, &[])]);
        assert_eq!(&message[12..], expected);
    }

    #[test]
    fn shared_object_send_callbacks_decode_amf0_and_amf3_references() {
        let mut amf0 = Vec::new();
        encode_amf0(&AmfValue::String("WAROVER".to_string()), &mut amf0).unwrap();
        encode_amf0(&AmfValue::String("victory".to_string()), &mut amf0).unwrap();
        let decoded = decode_shared_object(
            &shared_object_payload(19, "War_monster_60", 9, &[(6, &amf0)]),
            19,
        )
        .unwrap();
        assert!(matches!(
            decoded.events.as_slice(),
            [RtmpSharedObjectEvent::SendMessage { method, arguments }]
                if method == "WAROVER"
                    && arguments == &[AmfValue::String("victory".to_string())]
        ));

        let amf3 = b"\x06\x0fWAROVER\x06\x0fvictory\x06\x00".to_vec();
        let decoded = decode_shared_object(
            &shared_object_payload(16, "War_monster_60", 9, &[(6, &amf3)]),
            16,
        )
        .unwrap();
        assert!(matches!(
            decoded.events.as_slice(),
            [RtmpSharedObjectEvent::SendMessage { method, arguments }]
                if method == "WAROVER"
                    && arguments == &[
                        AmfValue::String("victory".to_string()),
                        AmfValue::String("WAROVER".to_string()),
                    ]
        ));
    }

    #[test]
    fn shared_object_change_decodes_captured_amf0_value() {
        let change = b"\x00\x04type\x02\x00\x07monster";
        let decoded = decode_shared_object(
            &shared_object_payload(19, "War_monster_60", 2, &[(4, change)]),
            19,
        )
        .unwrap();
        assert!(matches!(
            decoded.events.as_slice(),
            [RtmpSharedObjectEvent::Change { name, value }]
                if name == "type" && value == &AmfValue::String("monster".to_string())
        ));
    }

    #[test]
    fn shared_object_unknown_events_remain_ordered_and_lengths_are_bounded() {
        let decoded = decode_shared_object(
            &shared_object_payload(19, "x", 1, &[(99, b"abc"), (11, &[])]),
            19,
        )
        .unwrap();
        assert!(matches!(
            decoded.events.as_slice(),
            [
                RtmpSharedObjectEvent::Raw {
                    event_type: 99,
                    data
                },
                RtmpSharedObjectEvent::UseSuccess
            ] if data == b"abc"
        ));
        let mut malformed = shared_object_payload(19, "x", 1, &[(99, b"abc")]);
        malformed.pop();
        assert!(
            decode_shared_object(&malformed, 19)
                .unwrap_err()
                .contains("exceeds remaining")
        );
    }

    #[test]
    fn amf0_commands_embed_amf3_only_values() {
        let mut encoded = Vec::new();
        encode_amf0(&AmfValue::ByteArray(vec![1, 2, 3]), &mut encoded).unwrap();
        assert_eq!(encoded, b"\x11\x0c\x07\x01\x02\x03");
        let decoded = decode_amf0_sequence(&encoded).unwrap();
        assert!(matches!(
            decoded.as_slice(),
            [AmfValue::AMF3(value)] if value.as_ref() == &AmfValue::ByteArray(vec![1, 2, 3])
        ));
    }

    #[test]
    fn outbound_amf3_values_use_shared_string_references() {
        let mut encoder = Amf3Encoder::default();
        let mut encoded = Vec::new();
        for value in ["WAROVER", "victory", "WAROVER"] {
            encoder
                .encode(&AmfValue::String(value.to_string()), &mut encoded)
                .unwrap();
        }
        assert_eq!(encoded, b"\x06\x0fWAROVER\x06\x0fvictory\x06\x00");
    }

    #[test]
    fn remote_close_distinguishes_connected_from_connecting() {
        let mut connected = RtmpConnection::new(
            "rtmp://127.0.0.1/taoofms/s72.example".to_string(),
            Vec::new(),
        )
        .unwrap();
        connected.phase = Phase::Connected;
        connected.socket_closed();
        assert!(!connected.is_connected());
        assert!(matches!(
            connected.take_events().as_slice(),
            [RtmpEvent::Closed]
        ));
        assert!(connected.receive(vec![1, 2, 3]).is_err());
        assert!(connected.input.is_empty());

        let mut connecting = RtmpConnection::new(
            "rtmp://127.0.0.1/taoofms/s72.example".to_string(),
            Vec::new(),
        )
        .unwrap();
        connecting.phase = Phase::Handshake;
        connecting.socket_closed();
        assert!(matches!(
            connecting.take_events().as_slice(),
            [RtmpEvent::ProtocolError(_)]
        ));
    }

    #[test]
    fn malformed_control_and_command_messages_fail_connection() {
        let cases: &[(u8, &[u8])] = &[
            (1, &[0, 0, 1]),
            (1, &[0x80, 0, 0, 1]),
            (2, &[0, 0, 1]),
            (3, &[0, 0, 1]),
            (4, &[0]),
            (4, &[0, 6, 0, 0, 0]),
            (5, &[0, 0, 0, 0]),
            (6, &[0, 0, 0, 1]),
            (6, &[0, 0, 0, 1, 3]),
            (17, &[1]),
            (20, &[0xff]),
        ];
        for (message_type, payload) in cases {
            let mut connection = RtmpConnection::new(
                "rtmp://127.0.0.1/taoofms/s72.example".to_string(),
                Vec::new(),
            )
            .unwrap();
            connection.phase = Phase::Connected;
            let message = encode_message(payload, 3, *message_type, 0, 0, DEFAULT_CHUNK_SIZE);
            assert!(connection.receive(message).is_err(), "type {message_type}");
            assert_eq!(connection.phase, Phase::Failed);
            assert!(connection.input.is_empty());
            assert!(matches!(
                connection.take_events().as_slice(),
                [RtmpEvent::ProtocolError(_)]
            ));
        }
    }

    #[test]
    fn command_result_retains_transaction_and_value() {
        let mut connection = RtmpConnection::new(
            "rtmp://127.0.0.1/taoofms/s72.example".to_string(),
            Vec::new(),
        )
        .unwrap();
        connection.phase = Phase::Connected;
        let payload = command_payload("_result", 2.0, &[AmfValue::String("response".to_string())]);
        connection
            .receive(encode_message(&payload, 3, 20, 0, 0, DEFAULT_CHUNK_SIZE))
            .unwrap();
        assert!(matches!(
            connection.take_events().as_slice(),
            [RtmpEvent::Result {
                transaction_id: 2,
                value: AmfValue::String(value),
            }] if value == "response"
        ));
    }

    #[test]
    fn server_invoke_preserves_amf_arguments() {
        let mut connection = RtmpConnection::new(
            "rtmp://127.0.0.1/taoofms/s72.example".to_string(),
            Vec::new(),
        )
        .unwrap();
        connection.phase = Phase::Connected;
        let mut payload = command_payload("onAct", 2.0, &[]);
        // The live FMS embeds the onAct arguments as AMF3 values in a type-20
        // AMF0 command: the action string followed by a one-item dense array.
        payload.extend_from_slice(&[17, 6, 5, b'S', b'P']);
        payload.extend_from_slice(&[
            17, 9, 3, 1, 6, 17, b't', b'e', b's', b't', b'-', b'k', b'e', b'y',
        ]);
        let message = encode_message(&payload, 3, 20, 0, 0, DEFAULT_CHUNK_SIZE);
        connection.receive(message).unwrap();
        let events = connection.take_events();
        let RtmpEvent::Invoke {
            method,
            transaction_id,
            arguments,
        } = &events[0]
        else {
            panic!("expected invoke");
        };
        assert_eq!(method, "onAct");
        assert_eq!(*transaction_id, 2.0);
        assert_eq!(arguments.len(), 2);
        assert!(matches!(arguments[0], AmfValue::AMF3(_)));
        assert!(matches!(arguments[1], AmfValue::AMF3(_)));
    }
}
