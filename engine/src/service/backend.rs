use std::{
    collections::HashMap,
    sync::{Arc, RwLock as StdRwLock},
};

use enginelib::api::postcard;
use enginelib::{
    Identifier, RawIdentier, Registry,
    api::EngineAPI,
    chrono::Utc,
    events::{Events, ID},
    task::{StoredExecutingTask, StoredTask},
};
use tokio::sync::RwLock;
use tonic::{Response, Status};
use tracing::{debug, info, warn};

use crate::{
    get_auth, get_uid,
    proto::{self, TaskState, engine_server::Engine},
};

#[allow(non_snake_case)]
pub struct BackendEngineService {
    pub EngineAPI: Arc<RwLock<EngineAPI>>,
}

impl BackendEngineService {
    pub fn new(api: Arc<RwLock<EngineAPI>>) -> Self {
        Self { EngineAPI: api }
    }
}

pub fn mint_task_instance_id(node_id: Option<&str>) -> String {
    let random_hex = druid::Druid::default().to_hex();
    match node_id.filter(|value| !value.is_empty()) {
        Some(node_id) => format!("{node_id}@{random_hex}"),
        None => random_hex,
    }
}

pub fn parse_owner_node(task_instance_id: &str) -> Option<(&str, &str)> {
    let (node_id, local_id) = task_instance_id.split_once('@')?;
    if node_id.is_empty() || local_id.is_empty() || local_id.contains('@') {
        return None;
    }
    Some((node_id, local_id))
}

fn parse_task_key(task_id: &str) -> Result<Identifier, Status> {
    let Some((namespace, task)) = task_id.split_once(':') else {
        return Err(Status::invalid_argument(
            "Invalid task ID format, expected 'namespace:task'",
        ));
    };

    if namespace.is_empty() || task.is_empty() {
        return Err(Status::invalid_argument(
            "Invalid task ID format, expected 'namespace:task'",
        ));
    }

    Ok(ID(namespace, task))
}

fn delete_task_from_collection<T, F>(
    collection: &mut HashMap<(String, String), Vec<T>>,
    id: &(String, String),
    task_id: &str,
    state_name: &str,
    namespace: &str,
    task: &str,
    id_extractor: F,
) -> Result<(), Status>
where
    F: Fn(&T) -> &str,
{
    match collection.get_mut(id) {
        Some(query) => {
            let orig_len = query.len();
            query.retain(|f| id_extractor(f) != task_id);
            if query.len() == orig_len {
                info!(
                    "DeleteTask: Task with id {} not found in {} state for namespace: {}, task: {}",
                    task_id, state_name, namespace, task
                );
                return Err(Status::not_found(format!(
                    "Task with id {} not found in {} state",
                    task_id, state_name
                )));
            }
            Ok(())
        }
        None => {
            info!(
                "DeleteTask: No tasks found in {} state for namespace: {}, task: {}",
                state_name, namespace, task
            );
            Err(Status::not_found(format!(
                "No tasks found in {} state for given namespace and task",
                state_name
            )))
        }
    }
}

#[tonic::async_trait]
impl Engine for BackendEngineService {
    async fn check_auth(
        &self,
        request: tonic::Request<proto::Empty>,
    ) -> Result<Response<proto::Empty>, Status> {
        let challenge = get_auth(&request);
        let mut api = self.EngineAPI.write().await;
        let db = api.db.clone();
        let output = Events::CheckAdminAuth(&mut api, challenge, ("".into(), "".into()), db);
        if !output {
            warn!("Auth check failed - permission denied");
            return Err(Status::permission_denied("Invalid Auth"));
        }
        Ok(Response::new(proto::Empty {}))
    }

    async fn delete_task(
        &self,
        request: tonic::Request<proto::TaskSelector>,
    ) -> Result<Response<proto::Empty>, Status> {
        let mut api = self.EngineAPI.write().await;
        let data = request.get_ref();
        let challenge = get_auth(&request);
        let db = api.db.clone();
        let id = ID(&data.namespace, &data.task);

        let output = Events::CheckAdminAuth(&mut api, challenge, ("".into(), "".into()), db);
        if !output {
            warn!("Auth check failed - permission denied");
            return Err(Status::permission_denied("Invalid Auth"));
        }

        let result = match data.state() {
            TaskState::Processing => delete_task_from_collection(
                &mut api.executing_tasks.tasks,
                &id,
                &data.id,
                "Processing",
                &data.namespace,
                &data.task,
                |f| &f.id,
            ),
            TaskState::Solved => delete_task_from_collection(
                &mut api.solved_tasks.tasks,
                &id,
                &data.id,
                "Solved",
                &data.namespace,
                &data.task,
                |f| &f.id,
            ),
            TaskState::Queued => delete_task_from_collection(
                &mut api.task_queue.tasks,
                &id,
                &data.id,
                "Queued",
                &data.namespace,
                &data.task,
                |f| &f.id,
            ),
        };

        result?;
        EngineAPI::sync_db(&mut api);
        info!(
            "DeleteTask: Successfully deleted task with id {} in state {:?} for namespace: {}, task: {}",
            data.id,
            data.state(),
            data.namespace,
            data.task
        );
        Ok(Response::new(proto::Empty {}))
    }

    async fn get_tasks(
        &self,
        request: tonic::Request<proto::TaskPageRequest>,
    ) -> Result<Response<proto::TaskPage>, Status> {
        let mut api = self.EngineAPI.write().await;
        let challenge = get_auth(&request);
        let db = api.db.clone();
        if !Events::CheckAdminAuth(&mut api, challenge, ("".into(), "".into()), db) {
            info!("GetTask denied due to Invalid Auth");
            return Err(Status::permission_denied("Invalid authentication"));
        }

        let data = request.get_ref();
        let q: Vec<proto::Task> = match data.state() {
            TaskState::Processing => match api
                .executing_tasks
                .tasks
                .get(&(data.namespace.clone(), data.task.clone()))
            {
                Some(tasks) => {
                    let mut task_refs: Vec<_> = tasks.iter().collect();
                    task_refs.sort_by_key(|f| &f.id);
                    task_refs
                        .iter()
                        .map(|f| proto::Task {
                            id: f.id.clone(),
                            task_id: format!("{}:{}", data.namespace, data.task),
                            task_payload: f.bytes.clone(),
                            payload: Vec::new(),
                        })
                        .collect()
                }
                None => Vec::new(),
            },
            TaskState::Queued => match api
                .task_queue
                .tasks
                .get(&(data.namespace.clone(), data.task.clone()))
            {
                Some(tasks) => {
                    let mut tasks = tasks.clone();
                    tasks.sort_by_key(|f| f.id.clone());
                    tasks
                        .iter()
                        .map(|f| proto::Task {
                            id: f.id.clone(),
                            task_id: format!("{}:{}", data.namespace, data.task),
                            task_payload: f.bytes.clone(),
                            payload: Vec::new(),
                        })
                        .collect()
                }
                None => Vec::new(),
            },
            TaskState::Solved => match api
                .solved_tasks
                .tasks
                .get(&(data.namespace.clone(), data.task.clone()))
            {
                Some(tasks) => {
                    let mut tasks = tasks.clone();
                    tasks.sort_by_key(|f| f.id.clone());
                    tasks
                        .iter()
                        .map(|f| proto::Task {
                            id: f.id.clone(),
                            task_id: format!("{}:{}", data.namespace, data.task),
                            task_payload: f.bytes.clone(),
                            payload: Vec::new(),
                        })
                        .collect()
                }
                None => Vec::new(),
            },
        };

        let index = data.page.saturating_mul(data.page_size as u64) as usize;
        let limit = api.cfg.config_toml.pagination_limit.min(data.page_size) as usize;
        let final_vec: Vec<_> = q.iter().skip(index).take(limit).cloned().collect();
        Ok(Response::new(proto::TaskPage {
            namespace: data.namespace.clone(),
            task: data.task.clone(),
            page: data.page,
            page_size: data.page_size,
            state: data.state,
            tasks: final_vec,
        }))
    }

    async fn cgrpc(
        &self,
        request: tonic::Request<proto::Cgrpcmsg>,
    ) -> Result<Response<proto::Cgrpcmsg>, Status> {
        info!(
            "CGRPC request received for handler: {}:{}",
            request.get_ref().handler_mod_id,
            request.get_ref().handler_id
        );
        let mut api = self.EngineAPI.write().await;
        let challenge = get_auth(&request);
        let db = api.db.clone();
        debug!("Checking admin authentication for CGRPC request");
        let output = Events::CheckAdminAuth(
            &mut api,
            challenge,
            (
                request.get_ref().handler_mod_id.clone(),
                request.get_ref().handler_id.clone(),
            ),
            db,
        );
        if !output {
            warn!("CGRPC auth check failed - permission denied");
            return Err(Status::permission_denied("Invalid CGRPC Auth"));
        }
        let out = Arc::new(StdRwLock::new(Vec::new()));
        debug!("Dispatching CGRPC event to handler");
        Events::CgrpcEvent(
            &mut api,
            ID("engine_core", "grpc"),
            request.get_ref().event_payload.clone(),
            out.clone(),
        );
        let mut res = request.get_ref().clone();
        res.event_payload = match out.read() {
            Ok(g) => g.clone(),
            Err(_) => {
                warn!("CGRPC response lock poisoned, returning empty payload");
                Vec::new()
            }
        };
        info!("CGRPC request processed successfully");
        Ok(Response::new(res))
    }

    async fn aquire_task_reg(
        &self,
        request: tonic::Request<proto::Empty>,
    ) -> Result<Response<proto::TaskRegistry>, Status> {
        let uid = get_uid(&request);
        let challenge = get_auth(&request);
        info!("Task registry request received from user: {}", uid);
        let mut api = self.EngineAPI.write().await;
        let db = api.db.clone();

        debug!("Validating authentication for task registry request");
        if !Events::CheckAuth(&mut api, uid.clone(), challenge, db) {
            info!(
                "Task registry request denied - invalid authentication for user: {}",
                uid
            );
            return Err(Status::permission_denied("Invalid authentication"));
        }
        let mut tasks: Vec<RawIdentier> = api
            .task_registry
            .tasks
            .keys()
            .map(|(namespace, task)| format!("{namespace}:{task}"))
            .collect();
        tasks.sort();
        info!("Returning task registry with {} tasks", tasks.len());
        Ok(Response::new(proto::TaskRegistry { tasks }))
    }

    async fn aquire_task(
        &self,
        request: tonic::Request<proto::TaskRequest>,
    ) -> Result<Response<proto::Task>, Status> {
        let challenge = get_auth(&request);
        let input = request.get_ref();
        let task_id = input.task_id.clone();
        let uid = get_uid(&request);
        info!(
            "Task acquisition request received from user: {} for task: {}",
            uid, task_id
        );

        let mut api = self.EngineAPI.write().await;
        let db = api.db.clone();
        debug!("Validating authentication for task acquisition");
        if !Events::CheckAuth(&mut api, uid.clone(), challenge, db) {
            info!(
                "Task acquisition denied - invalid authentication for user: {}",
                uid
            );
            return Err(Status::permission_denied("Invalid authentication"));
        }

        let key = parse_task_key(&task_id)?;
        if api.task_registry.get(&key).is_none() {
            warn!(
                "Task acquisition failed - task does not exist: {}:{}",
                key.0, key.1
            );
            return Err(Status::invalid_argument("Task Does not Exist"));
        }

        let mut map = match api.task_queue.tasks.get(&key) {
            Some(v) if !v.is_empty() => v.clone(),
            _ => {
                info!("No queued tasks for {}:{}", key.0, key.1);
                return Err(Status::not_found("No queued tasks available"));
            }
        };

        let ttask = map.remove(0);
        let task_payload = ttask.bytes.clone();
        api.task_queue.tasks.insert(key.clone(), map);
        match postcard::to_allocvec(&api.task_queue.clone()) {
            Ok(store) => {
                if let Err(e) = api.db.insert("tasks", store) {
                    return Err(Status::internal(format!("DB insert error: {}", e)));
                }
            }
            Err(e) => {
                return Err(Status::internal(format!("Serialization error: {}", e)));
            }
        }

        let mut exec_tsks = api
            .executing_tasks
            .tasks
            .get(&key)
            .cloned()
            .unwrap_or_default();
        exec_tsks.push(StoredExecutingTask {
            bytes: task_payload.clone(),
            user_id: uid,
            given_at: Utc::now(),
            id: ttask.id.clone(),
        });
        api.executing_tasks.tasks.insert(key.clone(), exec_tsks);
        match postcard::to_allocvec(&api.executing_tasks.clone()) {
            Ok(store) => {
                if let Err(e) = api.db.insert("executing_tasks", store) {
                    return Err(Status::internal(format!("DB insert error: {}", e)));
                }
            }
            Err(e) => {
                return Err(Status::internal(format!("Serialization error: {}", e)));
            }
        }

        Ok(Response::new(proto::Task {
            id: ttask.id,
            task_id,
            task_payload,
            payload: Vec::new(),
        }))
    }

    async fn publish_task(
        &self,
        request: tonic::Request<proto::Task>,
    ) -> Result<Response<proto::Empty>, Status> {
        let mut api = self.EngineAPI.write().await;
        let challenge = get_auth(&request);
        let uid = get_uid(&request);
        let db = api.db.clone();

        let task = request.get_ref();
        let key = parse_task_key(&task.task_id)?;
        let instance_id = task.id.clone();

        if !Events::CheckAuth(&mut api, uid.clone(), challenge, db) {
            info!("Aquire Task denied due to Invalid Auth");
            return Err(Status::permission_denied("Invalid authentication"));
        }
        if !api.task_registry.tasks.contains_key(&key) {
            warn!(
                "Task acquisition failed - task does not exist: {}:{}",
                key.0, key.1
            );
            return Err(Status::invalid_argument("Task Does not Exist"));
        }

        let mem_tsk = api
            .executing_tasks
            .tasks
            .get(&key)
            .cloned()
            .unwrap_or_default();
        let executing_task = mem_tsk
            .iter()
            .find(|f| f.id == instance_id && f.user_id == uid)
            .cloned();
        let Some(executing_task) = executing_task else {
            return Err(Status::not_found("Invalid taskid or userid"));
        };

        let reg_tsk = match api.task_registry.get(&key) {
            Some(r) => r,
            None => {
                warn!("Task registry missing for {}:{}", key.0, key.1);
                return Err(Status::invalid_argument("Task Does not Exist"));
            }
        };
        if !reg_tsk.verify(task.task_payload.clone()) {
            info!("Failed to parse task");
            return Err(Status::invalid_argument("Failed to parse given task bytes"));
        }

        let mut nmem_tsk = mem_tsk.clone();
        nmem_tsk.retain(|f| !(f.id == instance_id && f.user_id == uid));
        api.executing_tasks.tasks.insert(key.clone(), nmem_tsk);
        match postcard::to_allocvec(&api.executing_tasks.clone()) {
            Ok(store) => {
                if let Err(e) = api.db.insert("executing_tasks", store) {
                    return Err(Status::internal(format!("DB insert error: {}", e)));
                }
            }
            Err(e) => return Err(Status::internal(format!("Serialization error: {}", e))),
        }

        let mut mem_solv = api
            .solved_tasks
            .tasks
            .get(&key)
            .cloned()
            .unwrap_or_default();
        mem_solv.push(StoredTask {
            bytes: task.task_payload.clone(),
            id: executing_task.id,
        });
        api.solved_tasks.tasks.insert(key.clone(), mem_solv);
        match postcard::to_allocvec(&api.solved_tasks.clone()) {
            Ok(store) => {
                if let Err(e) = api.db.insert("solved_tasks", store) {
                    return Err(Status::internal(format!("DB insert error: {}", e)));
                }
            }
            Err(e) => return Err(Status::internal(format!("Serialization error: {}", e))),
        }

        info!("Task published successfully: {} by user: {}", task.id, uid);
        Ok(Response::new(proto::Empty {}))
    }

    async fn create_task(
        &self,
        request: tonic::Request<proto::Task>,
    ) -> Result<Response<proto::Task>, Status> {
        let mut api = self.EngineAPI.write().await;
        let challenge = get_auth(&request);
        let uid = get_uid(&request);
        let db = api.db.clone();
        if !Events::CheckAuth(&mut api, uid, challenge, db) {
            info!("Create Task denied due to Invalid Auth");
            return Err(Status::permission_denied("Invalid authentication"));
        }
        let task = request.get_ref();
        let task_id = task.task_id.clone();
        let id = parse_task_key(&task_id)?;
        let tsk_reg = api.task_registry.get(&id);
        if let Some(tsk_reg) = tsk_reg {
            if !tsk_reg.verify(task.task_payload.clone()) {
                warn!("Failed to parse given task bytes");
                return Err(Status::invalid_argument("Failed to parse given task bytes"));
            }
            let stored_task = StoredTask {
                bytes: task.task_payload.clone(),
                id: mint_task_instance_id(api.cfg.config_toml.node_id.as_deref()),
            };
            let mut mem_task_queue = api.task_queue.clone();
            let mut mem_tasks = mem_task_queue.tasks.get(&id).cloned().unwrap_or_default();
            mem_tasks.push(stored_task.clone());
            mem_task_queue.tasks.insert(id.clone(), mem_tasks);
            api.task_queue = mem_task_queue;
            match postcard::to_allocvec(&api.task_queue.clone()) {
                Ok(store) => {
                    if let Err(e) = api.db.insert("tasks", store) {
                        return Err(Status::internal(format!("DB insert error: {}", e)));
                    }
                }
                Err(e) => return Err(Status::internal(format!("Serialization error: {}", e))),
            }
            return Ok(Response::new(proto::Task {
                id: stored_task.id.clone(),
                task_id,
                payload: Vec::new(),
                task_payload: stored_task.bytes.clone(),
            }));
        }
        Err(Status::aborted("Error"))
    }
}
