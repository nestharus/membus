use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::types::MembusError;

const WRITE_BATCH_MAX_FRAMES: usize = 64;
const WRITE_BATCH_MAX_BYTES: usize = 256 * 1024;

/// Multiplexed wire protocol version (`membus/2`).
pub const WIRE_VERSION: u16 = 2;

/// Fixed binary frame header length in bytes:
/// `[u32 body_len][u16 version=2][u8 frame_kind][u8 flags][u64 stream_id]`.
pub const FRAME_HEADER_LEN: usize = 16;

/// Default maximum permitted frame payload length (32 MiB).
pub const DEFAULT_MAX_FRAME_BODY: usize = 32 * 1024 * 1024;

/// Bitflags carried in the frame header.
pub struct FrameFlags;

impl FrameFlags {
    pub const NONE: u8 = 0;
    pub const ERROR: u8 = 0x01;
}

/// Discriminant for multiplexed wire frames.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    Request = 1,
    Response = 2,
    StreamEvent = 3,
    StreamEnd = 4,
    Ping = 5,
    Pong = 6,
    Cancel = 7,
}

impl FrameKind {
    pub fn from_u8(raw: u8) -> Result<Self, MembusError> {
        match raw {
            1 => Ok(Self::Request),
            2 => Ok(Self::Response),
            3 => Ok(Self::StreamEvent),
            4 => Ok(Self::StreamEnd),
            5 => Ok(Self::Ping),
            6 => Ok(Self::Pong),
            7 => Ok(Self::Cancel),
            other => Err(MembusError::Protocol(format!("unknown frame kind: {other}"))),
        }
    }
}

/// Decoded 16-byte binary header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    pub body_len: u32,
    pub version: u16,
    pub kind: FrameKind,
    pub flags: u8,
    pub stream_id: u64,
}

/// Complete binary frame multiplexed by `stream_id`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub kind: FrameKind,
    pub flags: u8,
    pub stream_id: u64,
    pub payload: Bytes,
}

impl Frame {
    pub fn new(kind: FrameKind, stream_id: u64, payload: Bytes) -> Self {
        Self {
            kind,
            flags: FrameFlags::NONE,
            stream_id,
            payload,
        }
    }

    pub fn with_flags(kind: FrameKind, flags: u8, stream_id: u64, payload: Bytes) -> Self {
        Self {
            kind,
            flags,
            stream_id,
            payload,
        }
    }
}

/// Encode `frame` directly into `dst` without intermediate copies, enforcing `max_body_len`.
pub fn encode_frame_into(
    frame: &Frame,
    dst: &mut BytesMut,
    max_body_len: usize,
) -> Result<(), MembusError> {
    if frame.payload.len() > max_body_len {
        return Err(MembusError::FrameTooLarge {
            len: frame.payload.len(),
            max_len: max_body_len,
        });
    }
    let body_len = u32::try_from(frame.payload.len()).map_err(|_| MembusError::FrameTooLarge {
        len: frame.payload.len(),
        max_len: max_body_len,
    })?;
    dst.reserve(FRAME_HEADER_LEN + frame.payload.len());
    dst.put_u32(body_len);
    dst.put_u16(WIRE_VERSION);
    dst.put_u8(frame.kind as u8);
    dst.put_u8(frame.flags);
    dst.put_u64(frame.stream_id);
    dst.put_slice(&frame.payload);
    Ok(())
}

/// Inspect and decode one complete frame from `buf` if available.
///
/// Validates header fields before consuming bytes so malformed frames never leave
/// partial corruption state behind.
pub fn try_decode_frame(
    buf: &mut BytesMut,
    max_body_len: usize,
) -> Result<Option<Frame>, MembusError> {
    if buf.len() < FRAME_HEADER_LEN {
        return Ok(None);
    }

    let body_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let version = u16::from_be_bytes([buf[4], buf[5]]);
    if version != WIRE_VERSION {
        return Err(MembusError::Protocol(format!(
            "unsupported wire version {version} (expected {WIRE_VERSION})"
        )));
    }
    let kind = FrameKind::from_u8(buf[6])?;
    let flags = buf[7];
    let stream_id = u64::from_be_bytes([
        buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
    ]);

    if body_len > max_body_len {
        return Err(MembusError::Protocol(format!(
            "frame body length {body_len} exceeds max_body_len {max_body_len}"
        )));
    }

    let total_len = FRAME_HEADER_LEN + body_len;
    if buf.len() < total_len {
        return Ok(None);
    }

    buf.advance(FRAME_HEADER_LEN);
    let payload = buf.split_to(body_len).freeze();
    Ok(Some(Frame {
        kind,
        flags,
        stream_id,
        payload,
    }))
}

/// Read the next frame asynchronously from `reader` using reusable scratch buffer `buf`.
///
/// Returns `Ok(None)` on clean EOF with no trailing partial frame bytes.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut BytesMut,
    max_body_len: usize,
) -> Result<Option<Frame>, MembusError> {
    loop {
        if let Some(frame) = try_decode_frame(buf, max_body_len)? {
            return Ok(Some(frame));
        }
        let n = reader.read_buf(buf).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err(MembusError::Protocol(format!(
                "connection closed mid-frame with {} trailing bytes",
                buf.len()
            )));
        }
    }
}

/// Drain framed messages from `rx` into batched writes on `writer` until the
/// channel closes or an I/O / encoding error occurs.
pub async fn run_batched_frame_writer<W: AsyncWrite + Unpin>(
    writer: &mut W,
    rx: &mut mpsc::Receiver<Frame>,
    max_body_len: usize,
) {
    let mut write_buf = BytesMut::with_capacity(64 * 1024);
    while let Some(first_frame) = rx.recv().await {
        write_buf.clear();
        if encode_frame_into(&first_frame, &mut write_buf, max_body_len).is_err() {
            break;
        }
        let mut count = 1usize;
        while count < WRITE_BATCH_MAX_FRAMES && write_buf.len() < WRITE_BATCH_MAX_BYTES {
            match rx.try_recv() {
                Ok(next_frame) => {
                    if encode_frame_into(&next_frame, &mut write_buf, max_body_len).is_err() {
                        let _ = writer.shutdown().await;
                        return;
                    }
                    count += 1;
                }
                Err(_) => break,
            }
        }
        if writer.write_all(&write_buf).await.is_err() {
            break;
        }
        if writer.flush().await.is_err() {
            break;
        }
    }
    let _ = writer.shutdown().await;
}
