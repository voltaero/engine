//! Reusable async DEALER client for the engine wire protocol.

use enginelib::Identifier;
use enginelib::api::StoredTask;
use enginelib::error::Error;
use enginelib::nucleus::query::{Query, QueryResult};
use enginelib::protocol::{Envelope, Request, Response, decode, encode};
use futures::{SinkExt, StreamExt};
use tmq::dealer::Dealer;
use tmq::{Context, Multipart, dealer};

/// A connected DEALER client. Each call sends one request and awaits one reply;
/// DEALER allows many in flight, but these methods are sequential per `&mut self`.
pub struct Client {
    sock: Dealer,
    auth: String,
}

impl Client {
    /// Connect a DEALER socket to `endpoint` (e.g. `tcp://[::1]:50051`).
    pub fn connect(endpoint: &str, auth: String) -> Result<Client, Error> {
        let ctx = Context::new();
        let sock = dealer(&ctx)
            .connect(endpoint)
            .map_err(|e| Error::io_error(format!("Failed to connect DEALER to {endpoint}: {e}")))?;
        Ok(Client { sock, auth })
    }

    /// Send one `Request`, await and decode the `Response`.
    async fn call(&mut self, req: Request) -> Result<Response, Error> {
        let env = Envelope {
            auth: self.auth.clone(),
            req,
        };
        let mp: Multipart = vec![encode(&env)?].into();
        self.sock
            .send(mp)
            .await
            .map_err(|e| Error::io_error(format!("send failed: {e}")))?;

        let reply = self
            .sock
            .next()
            .await
            .ok_or_else(|| Error::new("connection closed".into()))?
            .map_err(|e| Error::io_error(format!("recv failed: {e}")))?;
        let frame = reply
            .into_iter()
            .last()
            .ok_or_else(|| Error::new("empty reply".into()))?;
        decode(&frame)
    }

    /// Submit a batch of task payloads; returns the assigned ids.
    pub async fn submit(
        &mut self,
        task_type: Identifier,
        tasks: Vec<Vec<u8>>,
    ) -> Result<Vec<String>, Error> {
        match self.call(Request::SubmitBatch { task_type, tasks }).await? {
            Response::Submitted { task_ids } => Ok(task_ids),
            Response::Err(e) => Err(e),
            other => Err(unexpected(other)),
        }
    }

    /// Lease up to `max` tasks of one type. The server long-polls for the first
    /// task but gives up after its poll window; an empty reply just means
    /// "nothing arrived in time" — lease again.
    pub async fn lease(
        &mut self,
        task_type: Identifier,
        user_id: String,
        max: u32,
    ) -> Result<Vec<StoredTask>, Error> {
        match self
            .call(Request::LeaseBatch {
                task_type,
                user_id,
                max,
            })
            .await?
        {
            Response::Leased(tasks) => Ok(tasks),
            Response::Err(e) => Err(e),
            other => Err(unexpected(other)),
        }
    }

    /// Complete a batch of leased tasks; returns how many succeeded.
    pub async fn complete(
        &mut self,
        task_type: Identifier,
        results: Vec<(String, Vec<u8>)>,
    ) -> Result<u32, Error> {
        match self
            .call(Request::CompleteBatch { task_type, results })
            .await?
        {
            Response::Completed { ok } => Ok(ok),
            Response::Err(e) => Err(e),
            other => Err(unexpected(other)),
        }
    }

    /// Renew a single lease's TTL.
    pub async fn renew(&mut self, task_type: Identifier, task_id: String) -> Result<(), Error> {
        match self.call(Request::Renew { task_type, task_id }).await? {
            Response::Renewed => Ok(()),
            Response::Err(e) => Err(e),
            other => Err(unexpected(other)),
        }
    }

    /// Cancel a single lease (requeues the task).
    pub async fn cancel(&mut self, task_type: Identifier, task_id: String) -> Result<(), Error> {
        match self.call(Request::Cancel { task_type, task_id }).await? {
            Response::Cancelled => Ok(()),
            Response::Err(e) => Err(e),
            other => Err(unexpected(other)),
        }
    }

    /// Run an administrative query.
    pub async fn query(&mut self, q: Query) -> Result<QueryResult, Error> {
        match self.call(Request::Query(q)).await? {
            Response::Query(r) => Ok(r),
            Response::Err(e) => Err(e),
            other => Err(unexpected(other)),
        }
    }
}

fn unexpected(resp: Response) -> Error {
    Error::new(format!("unexpected response variant: {resp:?}"))
}
