//! Private stdin/stdout protocol helper for MIG-05 measurement tooling.
//!
//! Prepare its ordinary SDK connection before a fault/load phase. The only
//! accepted proof is this invocation's raw public-SDK SingleConversation RPC
//! response, never the optimistic local store or an unsolicited Push. Timeout
//! retires this process: the parent owns final process termination and reaping.
//! No tracing subscriber is installed; stdout is exclusively closed JSON lines.
//! Normal SDK bootstrap, convergence, and background session watchers remain
//! active. The SDK floors a configured zero reconnect budget to one attempt;
//! this helper does not claim a reconnect-free session or total wire-attempt
//! count. It only forbids adapter replacement and retries of this proof RPC.

#[path = "common/server_message_proof.rs"]
mod server_message_proof;

use std::io::{BufRead, Write};
use std::time::Duration;

use flare_im_core_sdk::prelude::{
    AccessConfig, DevelopmentEndpoints, IMClientBuilder, RuntimeComponents, SdkConfig,
    in_memory_im_provider,
};
use serde::Serialize;
use server_message_proof::{
    Config, MAX_LINE_BYTES, ProofResult, Request, State, bounded, classify_sdk_error,
    match_server_response, must_retire, recover_request_id,
};
use tokio::time::Instant;

#[derive(Serialize)]
struct Ready {
    schema: &'static str,
    state: &'static str,
    binding_sha256: String,
}

// READY establishes only that the normal SDK connect call completed. Its
// binding hash identifies the private fixture configuration, not a separate
// server-validated tenant claim or completed background bootstrap. CONNECT's
// actual tenant/user/device authority comes from the preissued trusted token.

enum Input {
    Line(Vec<u8>),
    End,
    Invalid,
}

fn read_line_bounded(reader: &mut impl BufRead) -> std::io::Result<Input> {
    let mut line = Vec::with_capacity(512);
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(if line.is_empty() {
                Input::End
            } else {
                Input::Invalid
            });
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let count = newline.map_or(available.len(), |index| index + 1);
        if line.len() + count > MAX_LINE_BYTES {
            return Ok(Input::Invalid);
        }
        line.extend_from_slice(&available[..count]);
        reader.consume(count);
        if newline.is_some() {
            line.pop();
            return Ok(Input::Line(line));
        }
    }
}

fn stdin_lines() -> tokio::sync::mpsc::Receiver<Input> {
    // At most one buffered command and one executing RPC. No SDK callback or
    // Tokio blocking-pool thread is waiting indefinitely on stdin at shutdown.
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    std::thread::spawn(move || {
        let input = std::io::stdin();
        let mut reader = input.lock();
        loop {
            let value = read_line_bounded(&mut reader).unwrap_or(Input::Invalid);
            let terminal = !matches!(value, Input::Line(_));
            if sender.blocking_send(value).is_err() || terminal {
                break;
            }
        }
    });
    receiver
}

fn emit(value: &impl Serialize) -> bool {
    let Ok(json) = serde_json::to_vec(value) else {
        return false;
    };
    if json.len() > 1_024 {
        return false;
    }
    let output = std::io::stdout();
    let mut locked = output.lock();
    locked.write_all(&json).is_ok() && locked.write_all(b"\n").is_ok() && locked.flush().is_ok()
}

async fn run() -> i32 {
    let started = Instant::now();
    let mut input = stdin_lines();
    let Ok(Some(Input::Line(line))) =
        bounded(started + Duration::from_secs(30), input.recv()).await
    else {
        return 78;
    };
    let Ok(config) = serde_json::from_slice::<Config>(&line) else {
        return 78;
    };
    if config.validate().is_err() {
        return 78;
    }
    drop(line); // Never log or retain the private input line after parsing.
    let lifetime = started + Duration::from_millis(config.lifetime_ms);
    let connect_deadline =
        lifetime.min(Instant::now() + Duration::from_millis(config.connect_timeout_ms));
    if config
        .check_local_resolution(connect_deadline)
        .await
        .is_err()
    {
        return 78;
    }
    // 只给 WebSocket 端点：协议计划里只有 ws，不会去试 QUIC。后台重连由链路状态机负责，不可关闭。
    let sdk_config = SdkConfig {
        access: Some(AccessConfig::development(DevelopmentEndpoints {
            ws_url: Some(config.ws_url.clone()),
            core_api_url: Some(config.http_url.clone()),
            media_url: Some(config.http_url.clone()),
            ..Default::default()
        })),
        tenant_id: Some(config.app_id.clone()),
        device_id: Some(config.device_id.clone()),
        connect_timeout_secs: Some(config.connect_timeout_ms.div_ceil(1_000)),
        tls_ca_cert_path: config.tls_ca_cert_path.clone(),
        ack_max_retries: Some(0),
        enable_metrics: false,
        ..Default::default()
    };
    let runtime = RuntimeComponents::native_default(sdk_config.clone(), in_memory_im_provider());
    let sender = runtime.transport.sender().clone();
    let Ok(client) = IMClientBuilder::new()
        .config(sdk_config)
        .runtime(runtime)
        .build()
    else {
        return 70;
    };
    match bounded(
        connect_deadline,
        client.connect(&config.user_id, Some(&config.token)),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(_)) => return 70,
        Err(()) => return 75,
    }
    if Instant::now() >= connect_deadline {
        return 75;
    }
    if !emit(&Ready {
        schema: "flare-sdk-proof-ready/1",
        state: "READY",
        binding_sha256: config.binding_sha256(),
    }) {
        return 74;
    }
    let exit = loop {
        let line = match bounded(lifetime, input.recv()).await {
            Ok(Some(Input::Line(line))) => line,
            Ok(Some(Input::End)) | Ok(None) => break 0,
            Ok(Some(Input::Invalid)) => break 78,
            Err(()) => break 75,
        };
        let request_started = Instant::now();
        let Some(request_id) = recover_request_id(&line) else {
            break 78;
        };
        let request = serde_json::from_slice::<Request>(&line);
        drop(line);
        let request = match request {
            Ok(request) if request.validate(&config).is_ok() => request,
            _ => {
                if !emit(&ProofResult::new(
                    request_id,
                    State::InvalidInput,
                    0,
                    request_started.elapsed(),
                )) {
                    break 74;
                }
                continue;
            }
        };
        let deadline = lifetime.min(request_started + Duration::from_millis(request.timeout_ms));
        let sync = request.expected.sync_request(&config.device_id);
        let result = bounded(
            deadline,
            sender.send_sync_and_wait(&sync, Duration::from_millis(request.timeout_ms)),
        )
        .await;
        let mut state = match result {
            Ok(Ok(response)) => match_server_response(&request.expected, &response),
            Ok(Err(error)) => classify_sdk_error(&error),
            Err(()) => State::Timeout,
        };
        if Instant::now() >= deadline {
            state = State::Timeout;
        }
        let mut result = ProofResult::new(request_id, state, 1, request_started.elapsed());
        if state == State::Verified {
            result.observation_sha256 = Some(request.expected.sha256());
        }
        if !emit(&result) {
            break 74;
        }
        if must_retire(state) {
            break 75;
        }
    };
    // This bounded attempt is not evidence that the transport actually closed:
    // the SDK can swallow lower-level teardown errors. The parent must reap.
    let disconnect_deadline = lifetime.min(Instant::now() + Duration::from_secs(2));
    let _ = bounded(disconnect_deadline, client.disconnect()).await;
    exit
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    std::process::exit(run().await);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn psp_016_stdin_is_bounded_and_requires_line_termination() {
        assert!(matches!(
            read_line_bounded(&mut std::io::Cursor::new(b"{}\n")),
            Ok(Input::Line(_))
        ));
        assert!(matches!(
            read_line_bounded(&mut std::io::Cursor::new(b"{}")),
            Ok(Input::Invalid)
        ));
        assert!(matches!(
            read_line_bounded(&mut std::io::Cursor::new(Vec::<u8>::new())),
            Ok(Input::End)
        ));
        assert!(matches!(
            read_line_bounded(&mut std::io::Cursor::new(vec![b'a'; MAX_LINE_BYTES + 1])),
            Ok(Input::Invalid)
        ));
    }
}
