//! ROUTER server loop: decodes wire requests and dispatches them to the nucleus.

use std::sync::Arc;

use enginelib::api::ServerAPI;
use enginelib::error::{Error, ErrorKind};
use enginelib::events::Events;
use enginelib::nucleus::{cancel, complete, lease, query, renew, submit};
use enginelib::protocol::{Envelope, Request, Response, decode, encode};
use futures::{SinkExt, StreamExt};
use tmq::{Context, Multipart, router};
use tokio::sync::mpsc;
use tracing::{error, warn};

/// Serve the nucleus over a ZeroMQ ROUTER socket bound to `endpoint`
/// (e.g. `tcp://[::1]:50051`) until the socket stream ends.
///
/// A single task owns the socket for both recv and send (ZeroMQ sockets are not
/// safe to share across tasks). Each request is handled on its own spawned task
/// and its reply is funneled back through an mpsc, so all socket I/O stays
/// single-owner. To scale past one socket, bind K of these over the same
/// `Arc<ServerAPI>` — the handler closure only needs the shared `Arc`.
pub async fn serve(api: Arc<ServerAPI>, endpoint: &str) -> Result<(), Error> {
    let ctx = Context::new();
    let mut socket = router(&ctx)
        .bind(endpoint)
        .map_err(|e| Error::io_error(format!("Failed to bind ROUTER on {endpoint}: {e}")))?;

    // Start the loader tasks: these move persisted records from the DB into the
    // lease channels. Submit only writes to the DB — the loaders are the queue.
    ServerAPI::spawn_loaders(&api);

    // (identity, reply_payload) produced by handler tasks, drained by this loop.
    let (reply_tx, mut reply_rx) = mpsc::unbounded_channel::<(Vec<u8>, Vec<u8>)>();

    loop {
        tokio::select! {
            // A handler finished → route its reply back to the right peer.
            Some((identity, payload)) = reply_rx.recv() => {
                let mp: Multipart = vec![identity, payload].into();
                if let Err(e) = socket.send(mp).await {
                    error!("ROUTER send failed: {e}");
                }
            }
            // A request arrived.
            incoming = socket.next() => {
                let Some(incoming) = incoming else { break; };
                let mut mp = match incoming {
                    Ok(mp) => mp,
                    Err(e) => { error!("ROUTER recv failed: {e}"); continue; }
                };
                // DEALER→ROUTER frames are [identity, .., payload]: identity is the
                // first frame, our single payload is the last.
                if mp.len() < 2 {
                    warn!("dropping malformed message ({} frames)", mp.len());
                    continue;
                }
                let identity = mp.pop_front().expect("len >= 2").to_vec();
                let payload = mp.pop_back().expect("len >= 2").to_vec();

                let api = api.clone();
                let reply_tx = reply_tx.clone();
                tokio::spawn(async move {
                    let response = handle(api, &payload).await;
                    let bytes = encode(&response).unwrap_or_else(|_| {
                        encode(&Response::Err(Error::new("failed to encode response".into())))
                            .unwrap_or_default()
                    });
                    let _ = reply_tx.send((identity, bytes));
                });
            }
        }
    }

    Ok(())
}

/// Reject helper for failed auth.
fn unauthorized() -> Response {
    Response::Err(Error::with_kind(ErrorKind::NotSupported, "auth rejected"))
}

/// Decode one envelope, authenticate, and dispatch to the nucleus.
async fn handle(api: Arc<ServerAPI>, payload: &[u8]) -> Response {
    let env: Envelope = match decode(payload) {
        Ok(env) => env,
        Err(e) => return Response::Err(e),
    };
    let Envelope { auth, req } = env;

    match req {
        Request::SubmitBatch { task_type, tasks } => {
            if !Events::CheckAuth(&api, String::new(), auth, api.db.clone()) {
                return unauthorized();
            }
            match submit::submit_batch(api, task_type, tasks) {
                Ok(task_ids) => Response::Submitted { task_ids },
                Err(e) => Response::Err(e),
            }
        }
        Request::LeaseBatch {
            task_type,
            user_id,
            max,
        } => {
            if !Events::CheckAuth(&api, user_id.clone(), auth, api.db.clone()) {
                return unauthorized();
            }
            match lease::lease_batch(api, task_type, user_id, max).await {
                Ok(tasks) => Response::Leased(tasks.into_iter().map(|t| (*t).clone()).collect()),
                Err(e) => Response::Err(e),
            }
        }
        Request::CompleteBatch {
            task_type,
            results,
        } => {
            if !Events::CheckAuth(&api, String::new(), auth, api.db.clone()) {
                return unauthorized();
            }
            match complete::complete_batch(api, task_type, results) {
                Ok(ok) => Response::Completed { ok },
                Err(e) => Response::Err(e),
            }
        }
        Request::Renew { task_type, task_id } => {
            if !Events::CheckAuth(&api, String::new(), auth, api.db.clone()) {
                return unauthorized();
            }
            match renew::renew(api, task_type, &task_id) {
                Ok(()) => Response::Renewed,
                Err(e) => Response::Err(e),
            }
        }
        Request::Cancel { task_type, task_id } => {
            if !Events::CheckAuth(&api, String::new(), auth, api.db.clone()) {
                return unauthorized();
            }
            match cancel::cancel(api, task_type, task_id) {
                Ok(()) => Response::Cancelled,
                Err(e) => Response::Err(e),
            }
        }
        Request::Query(q) => {
            // Query is the operator/admin surface → admin auth.
            let target = ("core".to_string(), "query".to_string());
            if !Events::CheckAdminAuth(&api, auth, target, api.db.clone()) {
                return unauthorized();
            }
            match query::query(api, q) {
                Ok(r) => Response::Query(r),
                Err(e) => Response::Err(e),
            }
        }
    }
}
