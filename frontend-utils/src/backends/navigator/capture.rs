//! Opt-in, application-level native network evidence. No URLs, headers or error text
//! enter the index. Payloads are private, and only consumed HTTP response bytes and
//! successful socket read/write slices are recorded (not TLS or redirect exchanges).
#[cfg(unix)]
use std::fs::{self, File, OpenOptions};
use std::io;
#[cfg(unix)]
use std::io::Write;
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
#[cfg(unix)]
use std::thread;
use std::thread::JoinHandle;
#[cfg(unix)]
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

const CHUNK: usize = 64 * 1024;
#[cfg(unix)]
const QUEUE: usize = 64;
const RUNNING: u8 = 0;
const SHUTDOWN: u8 = 1;
#[cfg(unix)]
const QUOTA: u8 = 2;
const QUEUE_FULL: u8 = 3;
#[cfg(unix)]
const DISK: u8 = 4;

struct State {
    stop: AtomicU8,
    sending: AtomicUsize,
    next_id: AtomicU64,
}

impl State {
    fn stop(&self, reason: u8) {
        if self
            .stop
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if current == RUNNING || (current == SHUTDOWN && reason != SHUTDOWN) {
                    Some(reason)
                } else {
                    None
                }
            })
            .is_ok()
            && reason != SHUTDOWN
        {
            eprintln!(
                "Network capture stopped; evidence is partial (quota, queue or storage failure); gameplay continues"
            );
        }
    }
}

struct Entry {
    time: u128,
    id: u64,
    event: &'static str,
    file: Option<String>,
    offset: u64,
    value: u64,
    bytes: Vec<u8>,
}

/// Application-owned writer. Drop stops accepting events, drains and joins even
/// when navigators or cancelled futures still hold producer handles.
pub struct NetworkCapture {
    handle: CaptureHandle,
    writer: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct CaptureHandle {
    sender: mpsc::SyncSender<Entry>,
    state: Arc<State>,
}

#[cfg(unix)]
fn private_file(path: &Path, append: bool) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true);
    if append {
        // The newly created 0700 session directory is exclusively owned by us.
        if let Ok(metadata) = fs::symlink_metadata(path) {
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(io::Error::other("Invalid capture file"));
            }
            options.append(true);
        } else {
            options.create_new(true);
        }
    } else {
        options.create_new(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

#[cfg(unix)]
fn private_directory(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    // Reject symlinks in ancestors too; do not canonicalize them away.
    for ancestor in path.ancestors().skip(1) {
        if !ancestor.as_os_str().is_empty()
            && fs::symlink_metadata(ancestor)?.file_type().is_symlink()
        {
            return Err(io::Error::other("Capture path must not contain symlinks"));
        }
    }
    fs::DirBuilder::new().mode(0o700).create(path)
}

impl NetworkCapture {
    pub fn new(directory: &Path, max_bytes: u64) -> io::Result<Self> {
        #[cfg(not(unix))]
        {
            let _ = (directory, max_bytes);
            Err(io::Error::other(
                "Private network capture is unsupported on this platform",
            ))
        }
        #[cfg(unix)]
        {
            if max_bytes == 0 {
                return Err(io::Error::other("Network capture budget must be positive"));
            }
            // A fresh directory avoids ever mixing sessions or truncating evidence.
            private_directory(directory).map_err(|_| {
                io::Error::other(
                    "Network capture requires a new private directory without symlinks",
                )
            })?;
            let initialize = || -> io::Result<_> {
                private_file(&directory.join(".gitignore"), false)?.write_all(b"*")?;
                private_file(&directory.join("capture.incomplete"), false)?;
                private_directory(&directory.join("http"))?;
                private_directory(&directory.join("connections"))?;
                private_file(&directory.join("network-index.jsonl"), false)
            };
            let index = initialize()
                .map_err(|_| io::Error::other("Could not initialize private network capture"))?;
            let state = Arc::new(State {
                stop: AtomicU8::new(RUNNING),
                sending: AtomicUsize::new(0),
                next_id: AtomicU64::new(1),
            });
            let (sender, receiver) = mpsc::sync_channel(QUEUE);
            let handle = CaptureHandle {
                sender,
                state: state.clone(),
            };
            let directory = directory.to_owned();
            let writer = thread::Builder::new()
                .name("network-capture".into())
                .spawn(move || write_capture(directory, index, max_bytes, receiver, state))
                .map_err(|_| io::Error::other("Could not start network capture writer"))?;
            handle.event(0, "session_start_v1", 0);
            Ok(Self {
                handle,
                writer: Some(writer),
            })
        }
    }

    pub fn handle(&self) -> CaptureHandle {
        self.handle.clone()
    }
}

impl Drop for NetworkCapture {
    fn drop(&mut self) {
        self.handle.event(0, "session_shutdown", 0);
        self.handle.state.stop(SHUTDOWN);
        if let Some(writer) = self.writer.take()
            && writer.join().is_err()
        {
            eprintln!("Network capture writer failed; evidence is partial");
        }
    }
}

impl CaptureHandle {
    fn send(&self, entry: Entry) {
        self.state.sending.fetch_add(1, Ordering::SeqCst);
        if self.state.stop.load(Ordering::SeqCst) == RUNNING && self.sender.try_send(entry).is_err()
        {
            self.state.stop(QUEUE_FULL);
        }
        self.state.sending.fetch_sub(1, Ordering::SeqCst);
    }

    fn event(&self, id: u64, event: &'static str, value: u64) {
        self.send(Entry {
            time: timestamp(),
            id,
            event,
            file: None,
            offset: 0,
            value,
            bytes: Vec::new(),
        });
    }

    fn bytes(&self, id: u64, file: &str, offset: &AtomicU64, bytes: &[u8]) {
        for chunk in bytes.chunks(CHUNK) {
            if self.state.stop.load(Ordering::SeqCst) != RUNNING {
                break;
            }
            self.send(Entry {
                time: timestamp(),
                id,
                event: "bytes",
                file: Some(file.to_owned()),
                offset: offset.fetch_add(chunk.len() as u64, Ordering::Relaxed),
                value: chunk.len() as u64,
                bytes: chunk.to_vec(),
            });
        }
    }

    pub(super) fn http(&self, post: bool, body: &[u8]) -> Arc<HttpCapture> {
        let id = self.state.next_id.fetch_add(1, Ordering::Relaxed);
        self.event(
            id,
            if post {
                "http_post_submitted"
            } else {
                "http_get_submitted"
            },
            0,
        );
        self.bytes(
            id,
            &format!("http/{id}.request.bin"),
            &AtomicU64::new(0),
            body,
        );
        self.event(id, "http_request_body_submitted", body.len() as u64);
        Arc::new(HttpCapture {
            capture: self.clone(),
            id,
            offset: AtomicU64::new(0),
            ended: AtomicBool::new(false),
        })
    }

    pub(super) fn socket(&self) -> Arc<SocketCapture> {
        let id = self.state.next_id.fetch_add(1, Ordering::Relaxed);
        self.event(id, "socket_attempt", 0);
        Arc::new(SocketCapture {
            capture: self.clone(),
            id,
            client: AtomicU64::new(0),
            server: AtomicU64::new(0),
        })
    }
}

pub(super) struct HttpCapture {
    capture: CaptureHandle,
    id: u64,
    offset: AtomicU64,
    ended: AtomicBool,
}

impl HttpCapture {
    pub(super) fn response(&self, status: u16) {
        self.capture
            .event(self.id, "http_final_response", status.into());
    }
    pub(super) fn bytes(&self, bytes: &[u8]) {
        self.capture.bytes(
            self.id,
            &format!("http/{}.response.bin", self.id),
            &self.offset,
            bytes,
        );
    }
    pub(super) fn finish(&self, event: &'static str) {
        if !self.ended.swap(true, Ordering::Relaxed) {
            self.capture.event(self.id, event, 0);
        }
    }
}

impl Drop for HttpCapture {
    fn drop(&mut self) {
        self.finish("http_cancelled_or_unconsumed");
    }
}

pub(super) struct SocketCapture {
    capture: CaptureHandle,
    id: u64,
    client: AtomicU64,
    server: AtomicU64,
}

impl SocketCapture {
    pub(super) fn event(&self, event: &'static str) {
        self.capture.event(self.id, event, 0);
    }
    pub(super) fn bytes(&self, client: bool, bytes: &[u8]) {
        let (direction, offset) = if client {
            ("client", &self.client)
        } else {
            ("server", &self.server)
        };
        self.capture.bytes(
            self.id,
            &format!("connections/{}.{direction}.bin", self.id),
            offset,
            bytes,
        );
    }
}

impl Drop for SocketCapture {
    fn drop(&mut self) {
        self.event("socket_closed_or_cancelled");
    }
}

fn timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(unix)]
fn write_capture(
    directory: PathBuf,
    mut index: File,
    max_bytes: u64,
    receiver: mpsc::Receiver<Entry>,
    state: Arc<State>,
) {
    let mut used = 1; // .gitignore; marker files are empty.
    let mut failed = false;
    loop {
        let entry = match receiver.recv_timeout(Duration::from_millis(25)) {
            Ok(entry) => entry,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if state.stop.load(Ordering::SeqCst) == RUNNING
                    || state.sending.load(Ordering::SeqCst) != 0
                {
                    continue;
                }
                // A producer may have queued its final entry between the timeout
                // and the stop check. No more can enqueue once sending is zero.
                match receiver.try_recv() {
                    Ok(entry) => entry,
                    Err(_) => break,
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        if failed {
            continue;
        }
        // All strings below are internal constants or generated numbered paths.
        let file = entry
            .file
            .as_ref()
            .map(|file| format!("\"{file}\""))
            .unwrap_or_else(|| "null".into());
        let line = format!(
            "{{\"time_ms\":{},\"id\":{},\"event\":\"{}\",\"file\":{},\"offset\":{},\"value\":{}}}\n",
            entry.time, entry.id, entry.event, file, entry.offset, entry.value
        );
        let length = (line.len() + entry.bytes.len()) as u64;
        if length > max_bytes.saturating_sub(used) {
            state.stop(QUOTA);
            failed = true;
            continue;
        }
        used += length;
        let write = || -> io::Result<()> {
            if let Some(file) = entry.file {
                private_file(&directory.join(file), true)?.write_all(&entry.bytes)?;
            }
            index.write_all(line.as_bytes())
        };
        if write().is_err() {
            state.stop(DISK);
            failed = true;
        }
    }
    if index.flush().is_err() {
        state.stop(DISK);
    }
    let reason = state.stop.load(Ordering::SeqCst);
    let marker = match reason {
        SHUTDOWN => "capture.closed",
        QUOTA => "capture.partial-quota",
        QUEUE_FULL => "capture.partial-queue",
        _ => "capture.partial-storage",
    };
    if fs::rename(directory.join("capture.incomplete"), directory.join(marker)).is_err() {
        eprintln!("Network capture finalization failed; evidence is partial");
    }
}

#[cfg(all(test, unix))]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::os::unix::fs::{MetadataExt, symlink};

    #[test]
    fn private_ordered_capture_flushes_with_live_handles() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("network");
        let capture = NetworkCapture::new(&dir, 1_000_000).unwrap();
        let handle = capture.handle();
        let http = handle.http(true, b"private request");
        http.response(200);
        http.bytes(b"one");
        http.bytes(b"two");
        http.finish("http_response_eof");
        let socket = handle.socket();
        socket.bytes(true, b"partial");
        socket.bytes(true, b" write");
        socket.bytes(false, b"reply");
        drop(socket);
        drop(capture); // Must join despite the producer and HTTP guard still living.
        assert_eq!(
            fs::read(dir.join("http/1.response.bin")).unwrap(),
            b"onetwo"
        );
        assert_eq!(
            fs::read(dir.join("connections/2.client.bin")).unwrap(),
            b"partial write"
        );
        assert_eq!(
            fs::read(dir.join("connections/2.server.bin")).unwrap(),
            b"reply"
        );
        let index = fs::read_to_string(dir.join("network-index.jsonl")).unwrap();
        assert!(index.contains("\"offset\":3,\"value\":3"));
        assert!(!index.contains("private request"));
        assert!(!index.contains("Authorization"));
        assert!(dir.join("capture.closed").exists());
        assert_eq!(fs::metadata(&dir).unwrap().mode() & 0o777, 0o700);
        assert_eq!(
            fs::metadata(dir.join("http/1.request.bin")).unwrap().mode() & 0o777,
            0o600
        );
        assert!(NetworkCapture::new(&dir, 1000).is_err());
        symlink(&dir, temp.path().join("link")).unwrap();
        assert!(NetworkCapture::new(&temp.path().join("link/child"), 1000).is_err());
    }

    #[test]
    fn quota_is_bounded_and_never_reports_complete() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("network");
        let capture = NetworkCapture::new(&dir, 1).unwrap();
        let socket = capture.handle().socket();
        socket.bytes(true, &[42; CHUNK + 1]);
        drop(capture);
        assert!(dir.join("capture.partial-quota").exists());
        assert!(!dir.join("capture.closed").exists());
        assert_eq!(
            fs::metadata(dir.join("network-index.jsonl")).unwrap().len(),
            0
        );
        assert_eq!(fs::read_dir(dir.join("connections")).unwrap().count(), 0);
        socket.bytes(false, b"gameplay may continue");

        let dir = temp.path().join("prefix");
        let capture = NetworkCapture::new(&dir, 512).unwrap();
        let socket = capture.handle().socket();
        socket.bytes(true, b"first");
        socket.bytes(true, &[42; CHUNK]);
        socket.bytes(true, b"not evidence");
        drop(capture);
        assert!(dir.join("capture.partial-quota").exists());
        assert_eq!(
            fs::read(dir.join("connections/1.client.bin")).unwrap(),
            b"first"
        );
        let file_bytes = fs::metadata(dir.join("network-index.jsonl")).unwrap().len()
            + fs::metadata(dir.join("connections/1.client.bin"))
                .unwrap()
                .len()
            + 1;
        assert!(file_bytes <= 512);
    }

    #[test]
    fn full_queue_stops_nonblocking_and_disk_failure_keeps_partial_marker() {
        let state = Arc::new(State {
            stop: AtomicU8::new(RUNNING),
            sending: AtomicUsize::new(0),
            next_id: AtomicU64::new(1),
        });
        let (sender, _receiver) = mpsc::sync_channel(1);
        let handle = CaptureHandle { sender, state };
        handle.event(0, "first", 0);
        handle.event(0, "overflow", 0);
        assert_eq!(handle.state.stop.load(Ordering::SeqCst), QUEUE_FULL);
        handle.event(0, "ignored", 0);

        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("network");
        let capture = NetworkCapture::new(&dir, 10_000).unwrap();
        // A directory where a numbered stream file should be forces a storage failure.
        fs::create_dir(dir.join("connections/1.client.bin")).unwrap();
        capture
            .handle()
            .socket()
            .bytes(true, b"still sent by gameplay");
        drop(capture);
        assert!(dir.join("capture.partial-storage").exists());
    }
}
