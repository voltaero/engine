//! Request authentication and nucleus dispatch for the ZeroMQ transport.

use std::sync::Arc;
use std::time::Duration;

use enginelib::api::ServerAPI;
use enginelib::config::TransportConfig;
use enginelib::error::{Error, ErrorKind};
use enginelib::events::Events;
use enginelib::nucleus::{cancel, complete, lease, query, renew, submit};
use enginelib::protocol::{Envelope, Request, Response};

/// Serve with the transport limits loaded into `ServerAPI`.
///
/// Background task loaders are deliberately started by the process/bootstrap
/// code, not here, so adding transport shards cannot accidentally duplicate
/// them.
pub async fn serve(api: Arc<ServerAPI>, endpoint: &str) -> Result<(), Error> {
    crate::transport::serve(api, endpoint).await
}

fn unauthorized() -> Response {
    Response::Err(Error::with_kind(ErrorKind::NotSupported, "auth rejected"))
}

fn handler_failed() -> Response {
    Response::Err(Error::new("request handler failed".into()))
}

/// Authenticate and dispatch one already decoded, admitted request.
pub(crate) async fn dispatch(
    api: Arc<ServerAPI>,
    env: Envelope,
    limits: TransportConfig,
) -> Response {
    let Envelope { auth, req } = env;

    match req {
        // Lease is the only operation that waits asynchronously. Authentication
        // is synchronous and mod-provided, so run it on Tokio's blocking pool.
        Request::LeaseBatch {
            task_type,
            user_id,
            max,
        } => {
            let auth_api = api.clone();
            let auth_db = api.db.clone();
            let auth_user = user_id.clone();
            let authorized = tokio::task::spawn_blocking(move || {
                Events::CheckAuth(&auth_api, auth_user, auth, auth_db)
            })
            .await
            .unwrap_or(false);
            if !authorized {
                return unauthorized();
            }

            // Leave encoding overhead beyond StoredTask's fields. The transport
            // performs a final exact encoded-size check as a second line of defense.
            let lease_bytes = limits.max_reply_bytes.saturating_sub(1024);
            match lease::lease_batch_limited(
                api,
                task_type,
                user_id,
                max,
                lease_bytes,
                Duration::from_millis(limits.lease_long_poll_ms),
            )
            .await
            {
                Ok(tasks) => {
                    Response::Leased(tasks.into_iter().map(|task| (*task).clone()).collect())
                }
                Err(err) => Response::Err(err),
            }
        }
        request => {
            // RocksDB and event handlers are synchronous. Keep them off Tokio
            // runtime workers; admission limits bound outstanding calls.
            tokio::task::spawn_blocking(move || dispatch_blocking(api, auth, request))
                .await
                .unwrap_or_else(|_| handler_failed())
        }
    }
}

fn dispatch_blocking(api: Arc<ServerAPI>, auth: String, req: Request) -> Response {
    match req {
        Request::SubmitBatch { task_type, tasks } => {
            if !Events::CheckAuth(&api, String::new(), auth, api.db.clone()) {
                return unauthorized();
            }
            match submit::submit_batch(api, task_type, tasks) {
                Ok(task_ids) => Response::Submitted { task_ids },
                Err(err) => Response::Err(err),
            }
        }
        Request::CompleteBatch { task_type, results } => {
            if !Events::CheckAuth(&api, String::new(), auth, api.db.clone()) {
                return unauthorized();
            }
            match complete::complete_batch(api, task_type, results) {
                Ok(ok) => Response::Completed { ok },
                Err(err) => Response::Err(err),
            }
        }
        Request::Renew { task_type, task_id } => {
            if !Events::CheckAuth(&api, String::new(), auth, api.db.clone()) {
                return unauthorized();
            }
            match renew::renew(api, task_type, &task_id) {
                Ok(()) => Response::Renewed,
                Err(err) => Response::Err(err),
            }
        }
        Request::Cancel { task_type, task_id } => {
            if !Events::CheckAuth(&api, String::new(), auth, api.db.clone()) {
                return unauthorized();
            }
            match cancel::cancel(api, task_type, task_id) {
                Ok(()) => Response::Cancelled,
                Err(err) => Response::Err(err),
            }
        }
        Request::Query(query_request) => {
            let target = ("core".to_string(), "query".to_string());
            if !Events::CheckAdminAuth(&api, auth, target, api.db.clone()) {
                return unauthorized();
            }
            match query::query(api, query_request) {
                Ok(result) => Response::Query(result),
                Err(err) => Response::Err(err),
            }
        }
        Request::LeaseBatch { .. } => {
            Response::Err(Error::new("lease dispatched on blocking path".into()))
        }
    }
}
