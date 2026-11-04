//! Browser HTTP transport via `fetch` (wasm32).

use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use js_sys::Uint8Array;
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use wasm_bindgen_futures::JsFuture;
use web_sys::{AbortController, Headers, Request, RequestInit, RequestMode, Response, Window};

use super::{HTTP_REQUEST_TIMEOUT, http_error_from_response_status, request_timed_out};
use crate::shared::error::{FlareError, Result};

/// `fetch` 的总时限：到点由 `AbortController` 中止请求（连同读响应体）。drop 时撤销定时器。
///
/// 浏览器的 `fetch` 自己不超时：网络卡住（切网、弱网、半开的 TCP）时请求会一直挂着，等它的
/// 调用方以及串行执行的 invoke 链都跟着卡死。原生端由 reqwest 的总时限兜住，这里补齐同一个时限。
struct FetchDeadline {
    window: Window,
    handle: i32,
    fired: Rc<Cell<bool>>,
    _on_timeout: Closure<dyn FnMut()>,
}

impl FetchDeadline {
    fn arm(window: &Window, init: &RequestInit, after: Duration) -> Result<Self> {
        let controller = AbortController::new()
            .map_err(|e| FlareError::system(format!("http abort controller init failed: {e:?}")))?;
        init.set_signal(Some(&controller.signal()));
        let fired = Rc::new(Cell::new(false));
        let on_timeout = {
            let fired = fired.clone();
            Closure::<dyn FnMut()>::new(move || {
                fired.set(true);
                controller.abort();
            })
        };
        let millis = i32::try_from(after.as_millis()).unwrap_or(i32::MAX);
        let handle = window
            .set_timeout_with_callback_and_timeout_and_arguments_0(
                on_timeout.as_ref().unchecked_ref(),
                millis,
            )
            .map_err(|e| FlareError::system(format!("http request timer failed: {e:?}")))?;
        Ok(Self {
            window: window.clone(),
            handle,
            fired,
            _on_timeout: on_timeout,
        })
    }

    fn fired(&self) -> bool {
        self.fired.get()
    }
}

impl Drop for FetchDeadline {
    fn drop(&mut self) {
        self.window.clear_timeout_with_handle(self.handle);
    }
}

pub fn build_url(base_url: &str, path: &str) -> String {
    if path.starts_with("http://") || path.starts_with("https://") {
        return path.to_string();
    }
    format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn append_query(url: String, query: Option<&HashMap<String, String>>) -> String {
    let Some(query) = query else {
        return url;
    };
    if query.is_empty() {
        return url;
    }
    let mut parts = Vec::with_capacity(query.len());
    for (key, value) in query {
        parts.push(format!(
            "{}={}",
            js_sys::encode_uri_component(key),
            js_sys::encode_uri_component(value)
        ));
    }
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}{}", parts.join("&"))
}

fn apply_headers(
    headers: &Headers,
    context_headers: &HashMap<String, String>,
    extra_headers: Option<&HashMap<String, String>>,
) -> Result<()> {
    for (key, value) in context_headers {
        headers
            .set(key, value)
            .map_err(|e| FlareError::system(format!("http set header failed: {e:?}")))?;
    }
    if let Some(extra) = extra_headers {
        for (key, value) in extra {
            headers
                .set(key, value)
                .map_err(|e| FlareError::system(format!("http set header failed: {e:?}")))?;
        }
    }
    Ok(())
}

pub async fn fetch_bytes(
    method: &str,
    url: String,
    query: Option<&HashMap<String, String>>,
    body: Option<Vec<u8>>,
    content_type: Option<&str>,
    context_headers: HashMap<String, String>,
    extra_headers: Option<&HashMap<String, String>>,
) -> Result<Vec<u8>> {
    let (bytes, _) = fetch_bytes_with_headers(
        method,
        url,
        query,
        body,
        content_type,
        context_headers,
        extra_headers,
    )
    .await?;
    Ok(bytes)
}

pub async fn fetch_bytes_with_headers(
    method: &str,
    url: String,
    query: Option<&HashMap<String, String>>,
    body: Option<Vec<u8>>,
    content_type: Option<&str>,
    context_headers: HashMap<String, String>,
    extra_headers: Option<&HashMap<String, String>>,
) -> Result<(Vec<u8>, HashMap<String, String>)> {
    // 浏览器 `fetch` 全程持有 `JsFuture`/`web_sys` 句柄，其 future 本质 `!Send`。
    // 若让它直接冒泡到 SyncTask::execute，会连带把 IMClient（`Arc<RwLock<Inner>>`，
    // 内含 `Vec<Arc<dyn SyncTask>>`）去 Send/Sync 化，波及全栈。这里把 `!Send` 工作
    // 搬进 `spawn_local`，对外只等待 oneshot 接收端（`Result<_, FlareError>` 是 Send），
    // 从而返回一个 Send future —— transport 边界一处收口，上层维持 Send + Sync。
    let method = method.to_string();
    let url = append_query(url, query);
    let content_type = content_type.map(|s| s.to_string());
    let extra_headers = extra_headers.cloned();

    let (tx, rx) = futures::channel::oneshot::channel();
    wasm_bindgen_futures::spawn_local(async move {
        let result = fetch_bytes_with_headers_local(
            &method,
            url,
            body,
            content_type.as_deref(),
            context_headers,
            extra_headers.as_ref(),
        )
        .await;
        // 接收端被丢弃（调用方取消）时忽略发送失败。
        let _ = tx.send(result);
    });
    rx.await
        .map_err(|_| FlareError::system("http fetch task dropped before completion"))?
}

/// `!Send` 内部实现：真正执行浏览器 `fetch`。仅经 [`fetch_bytes_with_headers`]
/// 在 `spawn_local` 内调用，`url` 已在外层完成 query 拼接。
async fn fetch_bytes_with_headers_local(
    method: &str,
    url: String,
    body: Option<Vec<u8>>,
    content_type: Option<&str>,
    context_headers: HashMap<String, String>,
    extra_headers: Option<&HashMap<String, String>>,
) -> Result<(Vec<u8>, HashMap<String, String>)> {
    let window =
        web_sys::window().ok_or_else(|| FlareError::system("browser window unavailable"))?;
    let headers = Headers::new()
        .map_err(|e| FlareError::system(format!("http headers init failed: {e:?}")))?;
    apply_headers(&headers, &context_headers, extra_headers)?;
    if let Some(content_type) = content_type {
        headers
            .set("Content-Type", content_type)
            .map_err(|e| FlareError::system(format!("http set content-type failed: {e:?}")))?;
    }

    let init = RequestInit::new();
    init.set_method(method);
    init.set_mode(RequestMode::Cors);
    init.set_headers(&headers);
    if let Some(body) = body {
        let array = Uint8Array::from(body.as_slice());
        init.set_body(array.as_ref());
    }

    let deadline = FetchDeadline::arm(&window, &init, HTTP_REQUEST_TIMEOUT)?;
    match fetch_and_read(&window, &init, method, &url).await {
        // 到点被中止的请求，fetch / 读响应体报的是 AbortError；按超时上报，与原生超时同类。
        Err(_) if deadline.fired() => Err(request_timed_out(method, &url, HTTP_REQUEST_TIMEOUT)),
        outcome => outcome,
    }
}

/// 发出请求并读完响应体；总时限由调用方的 [`FetchDeadline`] 负责。
async fn fetch_and_read(
    window: &Window,
    init: &RequestInit,
    method: &str,
    url: &str,
) -> Result<(Vec<u8>, HashMap<String, String>)> {
    let request = Request::new_with_str_and_init(url, init)
        .map_err(|e| FlareError::system(format!("http request init failed: {e:?}")))?;
    let resp_value = JsFuture::from(window.fetch_with_request(&request))
        .await
        .map_err(|e| {
            // 浏览器报告离线时 fetch 根本没发出去：标成 NetworkUnreachable，调用方可以放心重发
            // 一次性请求（刷新令牌）。在线时的失败分不清服务端是否已处理，仍按系统错误。
            let reason = format!("http fetch failed: {method} {url}: {e:?}");
            let offline = web_sys::window().is_some_and(|w| !w.navigator().on_line());
            if offline {
                FlareError::localized(crate::shared::error::ErrorCode::NetworkUnreachable, reason)
            } else {
                FlareError::system(reason)
            }
        })?;
    let resp: Response = resp_value
        .dyn_into()
        .map_err(|e| FlareError::system(format!("http response cast failed: {e:?}")))?;
    let status = resp.status();
    // 浏览器里 Rust 侧的 HTTP 结果原本完全不可见（fetch 在 devtools 网络面板能看到状态，
    // 但看不出核心把它当成了什么）。非 2xx 记 warn 带响应体 —— 网关的错误体只有
    // code/reason/message，不含凭据；2xx 记 debug，方便对账「请求发了、也成功了」。
    if !(200..300).contains(&status) {
        let body = JsFuture::from(
            resp.text()
                .map_err(|e| FlareError::system(format!("http read error body failed: {e:?}")))?,
        )
        .await
        .ok()
        .and_then(|v| v.as_string())
        .unwrap_or_default();
        tracing::warn!(
            method = %method,
            url = %url,
            status,
            body = %body,
            "http request failed"
        );
        return Err(http_error_from_response_status(status, &body));
    }
    tracing::debug!(method = %method, url = %url, status, "http request ok");
    let response_headers = collect_response_headers(resp.headers())?;
    let buffer = JsFuture::from(
        resp.array_buffer()
            .map_err(|e| FlareError::system(format!("http read body failed: {e:?}")))?,
    )
    .await
    .map_err(|e| FlareError::system(format!("http read body failed: {e:?}")))?;
    let array = Uint8Array::new(&buffer);
    Ok((array.to_vec(), response_headers))
}

/// 带上传进度的 PUT（直传对象存储用）。`fetch` 不暴露上传进度，这里走 XHR 的
/// `upload.onprogress`；和 [`fetch_bytes_with_headers`] 一样把 `!Send` 部分收进
/// `spawn_local`，对外返回 Send future。
pub async fn put_bytes_with_progress(
    url: String,
    body: Vec<u8>,
    extra_headers: HashMap<String, String>,
    on_sent: super::SentBytesCallback,
) -> Result<HashMap<String, String>> {
    let (tx, rx) = futures::channel::oneshot::channel();
    wasm_bindgen_futures::spawn_local(async move {
        let result = put_bytes_with_progress_local(url, body, extra_headers, on_sent).await;
        let _ = tx.send(result);
    });
    rx.await
        .map_err(|_| FlareError::system("http upload task dropped before completion"))?
}

async fn put_bytes_with_progress_local(
    url: String,
    body: Vec<u8>,
    extra_headers: HashMap<String, String>,
    on_sent: super::SentBytesCallback,
) -> Result<HashMap<String, String>> {
    use std::cell::RefCell;
    use std::rc::Rc;
    use wasm_bindgen::closure::Closure;
    use web_sys::{ProgressEvent, XmlHttpRequest};

    let total = body.len() as u64;
    let xhr =
        XmlHttpRequest::new().map_err(|e| FlareError::system(format!("xhr init failed: {e:?}")))?;
    xhr.open_with_async("PUT", &url, true)
        .map_err(|e| FlareError::system(format!("xhr open failed: {e:?}")))?;
    for (key, value) in &extra_headers {
        // 浏览器自己算 Content-Length，手动设会被拒（unsafe header）。
        if key.eq_ignore_ascii_case("content-length") {
            continue;
        }
        xhr.set_request_header(key, value)
            .map_err(|e| FlareError::system(format!("xhr set header failed: {e:?}")))?;
    }

    // 0 = 成功（load 事件），其余是失败原因；只取第一次结果。
    let (done_tx, done_rx) =
        futures::channel::oneshot::channel::<std::result::Result<(), String>>();
    let done_tx = Rc::new(RefCell::new(Some(done_tx)));
    let finish = {
        let done_tx = done_tx.clone();
        move |outcome: std::result::Result<(), String>| {
            if let Some(tx) = done_tx.borrow_mut().take() {
                let _ = tx.send(outcome);
            }
        }
    };
    let on_load = {
        let finish = finish.clone();
        Closure::<dyn FnMut()>::new(move || finish(Ok(())))
    };
    let on_error = {
        let finish = finish.clone();
        Closure::<dyn FnMut()>::new(move || finish(Err("network error".to_string())))
    };
    let on_abort = {
        let finish = finish.clone();
        Closure::<dyn FnMut()>::new(move || finish(Err("aborted".to_string())))
    };
    let on_progress = {
        let on_sent = on_sent.clone();
        Closure::<dyn FnMut(ProgressEvent)>::new(move |event: ProgressEvent| {
            on_sent((event.loaded() as u64).min(total));
        })
    };
    xhr.set_onload(Some(on_load.as_ref().unchecked_ref()));
    xhr.set_onerror(Some(on_error.as_ref().unchecked_ref()));
    xhr.set_onabort(Some(on_abort.as_ref().unchecked_ref()));
    xhr.set_ontimeout(Some(on_error.as_ref().unchecked_ref()));
    let upload = xhr
        .upload()
        .map_err(|e| FlareError::system(format!("xhr upload handle failed: {e:?}")))?;
    upload.set_onprogress(Some(on_progress.as_ref().unchecked_ref()));

    let array = Uint8Array::from(body.as_slice());
    xhr.send_with_opt_buffer_source(Some(array.as_ref()))
        .map_err(|e| FlareError::system(format!("xhr send failed: {e:?}")))?;
    let outcome = done_rx
        .await
        .unwrap_or_else(|_| Err("upload callback dropped".to_string()));

    xhr.set_onload(None);
    xhr.set_onerror(None);
    xhr.set_onabort(None);
    xhr.set_ontimeout(None);
    upload.set_onprogress(None);
    drop((on_load, on_error, on_abort, on_progress));

    if let Err(reason) = outcome {
        let reason = format!("http upload failed: PUT {url}: {reason}");
        let offline = web_sys::window().is_some_and(|w| !w.navigator().on_line());
        return Err(if offline {
            FlareError::localized(crate::shared::error::ErrorCode::NetworkUnreachable, reason)
        } else {
            FlareError::system(reason)
        });
    }
    let status = xhr
        .status()
        .map_err(|e| FlareError::system(format!("xhr status failed: {e:?}")))?;
    if !(200..300).contains(&status) {
        let body = xhr.response_text().ok().flatten().unwrap_or_default();
        tracing::warn!(url = %url, status, body = %body, "http upload failed");
        return Err(http_error_from_response_status(status, &body));
    }
    tracing::debug!(url = %url, status, "http upload ok");
    let mut headers = HashMap::new();
    let raw = xhr.get_all_response_headers().unwrap_or_default();
    for line in raw.split("\r\n") {
        if let Some((key, value)) = line.split_once(':') {
            let key = key.trim().to_ascii_lowercase();
            if !key.is_empty() {
                headers.insert(key, value.trim().to_string());
            }
        }
    }
    on_sent(total);
    Ok(headers)
}

fn collect_response_headers(headers: Headers) -> Result<HashMap<String, String>> {
    let mut out = HashMap::new();
    let iterator = js_sys::try_iter(headers.as_ref())
        .map_err(|e| FlareError::system(format!("http response headers iter failed: {e:?}")))?
        .ok_or_else(|| FlareError::system("http response headers are not iterable"))?;
    for item in iterator {
        let entry = item
            .map_err(|e| FlareError::system(format!("http response header item failed: {e:?}")))?;
        let pair = js_sys::Array::from(&entry);
        if pair.length() < 2 {
            continue;
        }
        let key = pair.get(0).as_string().unwrap_or_default();
        if key.is_empty() {
            continue;
        }
        let value = pair.get(1).as_string().unwrap_or_default();
        out.insert(key, value);
    }
    Ok(out)
}

pub async fn fetch_json<T>(
    method: &str,
    url: String,
    query: Option<&HashMap<String, String>>,
    body: Option<&str>,
    context_headers: HashMap<String, String>,
    extra_headers: Option<&HashMap<String, String>>,
) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let bytes = fetch_bytes(
        method,
        url,
        query,
        body.map(|s| s.as_bytes().to_vec()),
        body.map(|_| "application/json"),
        context_headers,
        extra_headers,
    )
    .await?;
    serde_json::from_slice(&bytes)
        .map_err(|e| FlareError::system(format!("parse http json failed: {e}")))
}
