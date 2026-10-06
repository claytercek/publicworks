use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::JoinHandle,
};

#[derive(Clone, Debug)]
pub struct Request {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}
impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

pub enum Reply {
    Bytes(Vec<u8>),
    DelayHeaders {
        delay: Duration,
        bytes: Vec<u8>,
    },
    DelayBody {
        head: Vec<u8>,
        prefix: Vec<u8>,
        delay: Duration,
        suffix: Vec<u8>,
    },
    Stall {
        head: Option<Vec<u8>>,
        prefix: Vec<u8>,
        entered: Arc<Notify>,
        peer_closed: Arc<AtomicBool>,
    },
}

pub struct Server {
    endpoint: String,
    requests: Arc<Mutex<Vec<Request>>>,
    task: JoinHandle<()>,
}
impl Server {
    pub async fn start(path: &str, replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = format!("http://{address}{path}");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let task = tokio::spawn(async move {
            let mut replies: VecDeque<_> = replies.into();
            while let Some(reply) = replies.pop_front() {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let request = read_request(&mut stream).await.unwrap();
                captured.lock().unwrap().push(request);
                match reply {
                    Reply::Bytes(bytes) => {
                        let _ = stream.write_all(&bytes).await;
                    }
                    Reply::DelayHeaders { delay, bytes } => {
                        tokio::time::sleep(delay).await;
                        let _ = stream.write_all(&bytes).await;
                    }
                    Reply::DelayBody {
                        head,
                        prefix,
                        delay,
                        suffix,
                    } => {
                        let _ = stream.write_all(&head).await;
                        let _ = stream.write_all(&prefix).await;
                        let _ = stream.flush().await;
                        tokio::time::sleep(delay).await;
                        let _ = stream.write_all(&suffix).await;
                    }
                    Reply::Stall {
                        head,
                        prefix,
                        entered,
                        peer_closed,
                    } => {
                        if let Some(head) = head {
                            let _ = stream.write_all(&head).await;
                            let _ = stream.write_all(&prefix).await;
                            let _ = stream.flush().await;
                        }
                        entered.notify_one();
                        let mut byte = [0_u8; 1];
                        if matches!(stream.read(&mut byte).await, Ok(0) | Err(_)) {
                            peer_closed.store(true, Ordering::SeqCst);
                        }
                    }
                }
            }
        });
        Self {
            endpoint,
            requests,
            task,
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn requests(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub fn response(status: u16, headers: &[(&str, &str)], body: &str) -> Vec<u8> {
    let reason = match status {
        200 => "OK",
        301 => "Moved Permanently",
        302 => "Found",
        307 => "Temporary Redirect",
        401 => "Unauthorized",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Test Response",
    };
    let mut bytes = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    )
    .into_bytes();
    for (name, value) in headers {
        bytes.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    bytes.extend_from_slice(b"\r\n");
    bytes.extend_from_slice(body.as_bytes());
    bytes
}

pub fn body_head(content_length: usize) -> Vec<u8> {
    format!("HTTP/1.1 200 OK\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n")
        .into_bytes()
}

async fn read_request(stream: &mut TcpStream) -> std::io::Result<Request> {
    let mut bytes = Vec::new();
    let header_end = loop {
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        if bytes.len() > 64 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "test request headers too large",
            ));
        }
        let mut chunk = [0_u8; 4096];
        let count = stream.read(&mut chunk).await?;
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "request ended before headers",
            ));
        }
        bytes.extend_from_slice(&chunk[..count]);
    };
    let head = std::str::from_utf8(&bytes[..header_end - 4])
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next().unwrap_or_default().split_whitespace();
    let method = request_line.next().unwrap_or_default().to_owned();
    let target = request_line.next().unwrap_or_default().to_owned();
    let headers = lines
        .map(|line| {
            let (name, value) = line.split_once(':').ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid request header")
            })?;
            Ok((name.to_owned(), value.trim().to_owned()))
        })
        .collect::<std::io::Result<Vec<_>>>()?;
    let content_length = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.parse::<usize>().ok())
        .unwrap_or(0);
    while bytes.len() < header_end + content_length {
        let mut chunk = [0_u8; 4096];
        let count = stream.read(&mut chunk).await?;
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "request ended before body",
            ));
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    Ok(Request {
        method,
        target,
        headers,
        body: bytes[header_end..header_end + content_length].to_vec(),
    })
}
