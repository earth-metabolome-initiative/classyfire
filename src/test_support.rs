use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct MockResponse {
    status: u16,
    content_type: String,
    body: String,
}

impl MockResponse {
    pub fn json(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type: "application/json".to_owned(),
            body: body.into(),
        }
    }

    pub fn text(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type: "text/plain".to_owned(),
            body: body.into(),
        }
    }

    pub fn html(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type: "text/html".to_owned(),
            body: body.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SeenRequest {
    pub path: String,
    pub headers: HashMap<String, String>,
    pub body: String,
}

pub struct MockServer {
    address: SocketAddr,
    seen_requests: Arc<Mutex<Vec<SeenRequest>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl MockServer {
    pub fn new(routes: impl IntoIterator<Item = (impl Into<String>, MockResponse)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("failed binding mock server");
        listener
            .set_nonblocking(true)
            .expect("failed setting mock server nonblocking");
        let address = listener.local_addr().expect("missing mock server address");
        let routes = Arc::new(
            routes
                .into_iter()
                .map(|(path, response)| (path.into(), response))
                .collect::<HashMap<_, _>>(),
        );
        let seen_requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let thread_routes = Arc::clone(&routes);
        let thread_seen_requests = Arc::clone(&seen_requests);
        let thread_stop = Arc::clone(&stop);
        let handle = thread::spawn(move || loop {
            if thread_stop.load(Ordering::SeqCst) {
                break;
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    handle_connection(stream, &thread_routes, &thread_seen_requests)
                        .expect("mock server connection handling failed");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("mock server accept failed: {error}"),
            }
        });

        Self {
            address,
            seen_requests,
            stop,
            handle: Some(handle),
        }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    pub fn seen_paths(&self) -> Vec<String> {
        self.seen_requests()
            .into_iter()
            .map(|request| request.path)
            .collect()
    }

    pub fn seen_requests(&self) -> Vec<SeenRequest> {
        self.seen_requests
            .lock()
            .expect("mock server seen_requests mutex poisoned")
            .clone()
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        if let Some(handle) = self.handle.take() {
            handle.join().expect("mock server thread panicked");
        }
    }
}

fn handle_connection(
    stream: TcpStream,
    routes: &HashMap<String, MockResponse>,
    seen_requests: &Mutex<Vec<SeenRequest>>,
) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(());
    }

    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .to_owned();
    let mut headers = HashMap::new();
    loop {
        let mut header_line = String::new();
        if reader.read_line(&mut header_line)? == 0 || header_line == "\r\n" {
            break;
        }
        if let Some((name, value)) = header_line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }

    let content_length = headers
        .get("content-length")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = vec![0_u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }

    seen_requests
        .lock()
        .expect("mock server seen_requests mutex poisoned")
        .push(SeenRequest {
            path: path.clone(),
            headers,
            body: String::from_utf8_lossy(&body).into_owned(),
        });

    let response = routes
        .get(&path)
        .cloned()
        .unwrap_or_else(|| MockResponse::text(404, "missing route"));
    let status_text = status_text(response.status);
    let mut writer = reader.into_inner();
    write!(
        writer,
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        response.status,
        status_text,
        response.content_type,
        response.body.len(),
        response.body
    )?;
    writer.flush()
}

fn status_text(status: u16) -> &'static str {
    match status {
        200 => "OK",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        _ => "Test Response",
    }
}
