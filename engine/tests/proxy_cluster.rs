mod common;

use common::{
    BoxError, admin_request, cluster_client, cluster_request, engine_client, spawn_backend,
    spawn_proxy, task_bytes, wait_for_node_count, worker_request,
};
use engine::proto;
use enginelib::{
    chrono::Utc,
    events::ID,
    task::{StoredExecutingTask, StoredTask},
};
use tokio::task::JoinSet;
use tonic::Code;

#[tokio::test]
async fn create_task_distributes_across_nodes() -> Result<(), BoxError> {
    let proxy = spawn_proxy().await?;
    let backend_a = spawn_backend("node-a", &["auth", "control"], &proxy.addr).await?;
    let backend_b = spawn_backend("node-b", &["gpu"], &proxy.addr).await?;

    let mut join_set = JoinSet::new();
    for value in 0..32 {
        let proxy_addr = proxy.addr.clone();
        join_set.spawn(async move {
            let mut client = engine_client(&proxy_addr).await.unwrap();
            client
                .create_task(
                    worker_request(proto::Task {
                        id: String::new(),
                        task_id: "dist:work".into(),
                        task_payload: task_bytes(value, "dist", "work"),
                        payload: Vec::new(),
                    })
                    .unwrap(),
                )
                .await
        });
    }
    while let Some(result) = join_set.join_next().await {
        result??;
    }

    let queued_a = backend_a
        .api
        .read()
        .await
        .task_queue
        .tasks
        .get(&ID("dist", "work"))
        .unwrap()
        .len();
    let queued_b = backend_b
        .api
        .read()
        .await
        .task_queue
        .tasks
        .get(&ID("dist", "work"))
        .unwrap()
        .len();
    assert!(queued_a > 0, "expected node-a to receive tasks");
    assert!(queued_b > 0, "expected node-b to receive tasks");

    backend_a.shutdown().await;
    backend_b.shutdown().await;
    proxy.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn aquire_task_retries_until_a_node_has_work() -> Result<(), BoxError> {
    let proxy = spawn_proxy().await?;
    let backend_a = spawn_backend("node-a", &["auth", "control"], &proxy.addr).await?;
    let backend_b = spawn_backend("node-b", &["gpu"], &proxy.addr).await?;

    backend_b
        .api
        .write()
        .await
        .task_queue
        .tasks
        .get_mut(&ID("dist", "work"))
        .unwrap()
        .push(StoredTask {
            id: "node-b@queued-1".into(),
            bytes: task_bytes(1, "dist", "work"),
        });

    let mut client = engine_client(&proxy.addr).await?;
    let response = client
        .aquire_task(worker_request(proto::TaskRequest {
            task_id: "dist:work".into(),
        })?)
        .await?
        .into_inner();

    assert_eq!(response.id, "node-b@queued-1");
    assert_eq!(
        backend_b
            .api
            .read()
            .await
            .executing_tasks
            .tasks
            .get(&ID("dist", "work"))
            .unwrap()
            .len(),
        1
    );

    backend_a.shutdown().await;
    backend_b.shutdown().await;
    proxy.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn publish_task_routes_by_owner_prefix() -> Result<(), BoxError> {
    let proxy = spawn_proxy().await?;
    let backend_a = spawn_backend("node-a", &["auth", "control"], &proxy.addr).await?;
    let backend_b = spawn_backend("node-b", &["gpu"], &proxy.addr).await?;

    let mut client = engine_client(&proxy.addr).await?;
    let created = client
        .create_task(worker_request(proto::Task {
            id: String::new(),
            task_id: "dist:work".into(),
            task_payload: task_bytes(7, "dist", "work"),
            payload: Vec::new(),
        })?)
        .await?
        .into_inner();
    let acquired = client
        .aquire_task(worker_request(proto::TaskRequest {
            task_id: "dist:work".into(),
        })?)
        .await?
        .into_inner();
    assert_eq!(created.id, acquired.id);

    client
        .publish_task(worker_request(proto::Task {
            id: acquired.id.clone(),
            task_id: acquired.task_id.clone(),
            task_payload: task_bytes(99, "dist", "work"),
            payload: Vec::new(),
        })?)
        .await?;

    let owner = acquired.id.split('@').next().unwrap();
    let solved_a = backend_a
        .api
        .read()
        .await
        .solved_tasks
        .tasks
        .get(&ID("dist", "work"))
        .unwrap()
        .len();
    let solved_b = backend_b
        .api
        .read()
        .await
        .solved_tasks
        .tasks
        .get(&ID("dist", "work"))
        .unwrap()
        .len();
    match owner {
        "node-a" => {
            assert_eq!(solved_a, 1);
            assert_eq!(solved_b, 0);
        }
        "node-b" => {
            assert_eq!(solved_a, 0);
            assert_eq!(solved_b, 1);
        }
        _ => panic!("unexpected owner {owner}"),
    }

    backend_a.shutdown().await;
    backend_b.shutdown().await;
    proxy.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn delete_task_routes_by_owner_prefix() -> Result<(), BoxError> {
    let proxy = spawn_proxy().await?;
    let backend_a = spawn_backend("node-a", &["auth", "control"], &proxy.addr).await?;
    let backend_b = spawn_backend("node-b", &["gpu"], &proxy.addr).await?;

    let mut client = engine_client(&proxy.addr).await?;
    let created = client
        .create_task(worker_request(proto::Task {
            id: String::new(),
            task_id: "dist:work".into(),
            task_payload: task_bytes(8, "dist", "work"),
            payload: Vec::new(),
        })?)
        .await?
        .into_inner();

    client
        .delete_task(admin_request(proto::TaskSelector {
            state: proto::TaskState::Queued as i32,
            namespace: "dist".into(),
            task: "work".into(),
            id: created.id.clone(),
        })?)
        .await?;

    let owner = created.id.split('@').next().unwrap();
    let queued_a = backend_a
        .api
        .read()
        .await
        .task_queue
        .tasks
        .get(&ID("dist", "work"))
        .unwrap()
        .len();
    let queued_b = backend_b
        .api
        .read()
        .await
        .task_queue
        .tasks
        .get(&ID("dist", "work"))
        .unwrap()
        .len();
    match owner {
        "node-a" => assert_eq!(queued_a + queued_b, 0),
        "node-b" => assert_eq!(queued_a + queued_b, 0),
        _ => panic!("unexpected owner {owner}"),
    }

    backend_a.shutdown().await;
    backend_b.shutdown().await;
    proxy.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn aquire_task_reg_returns_union_of_registered_tasks() -> Result<(), BoxError> {
    let proxy = spawn_proxy().await?;
    let backend_a = spawn_backend("node-a", &["auth", "control"], &proxy.addr).await?;
    let backend_b = spawn_backend("node-b", &["gpu"], &proxy.addr).await?;

    let mut client = engine_client(&proxy.addr).await?;
    let mut tasks = client
        .aquire_task_reg(worker_request(proto::Empty {})?)
        .await?
        .into_inner()
        .tasks;
    tasks.sort();

    assert_eq!(
        tasks,
        vec![
            "dist:work".to_string(),
            "ml:train".to_string(),
            "node:node-a".to_string(),
            "node:node-b".to_string(),
        ]
    );

    backend_a.shutdown().await;
    backend_b.shutdown().await;
    proxy.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn get_tasks_fanout_merges_and_paginates() -> Result<(), BoxError> {
    let proxy = spawn_proxy().await?;
    let backend_a = spawn_backend("node-a", &["auth", "control"], &proxy.addr).await?;
    let backend_b = spawn_backend("node-b", &["gpu"], &proxy.addr).await?;

    backend_a
        .api
        .write()
        .await
        .task_queue
        .tasks
        .get_mut(&ID("dist", "work"))
        .unwrap()
        .extend([
            StoredTask {
                id: "node-a@0001".into(),
                bytes: task_bytes(1, "dist", "work"),
            },
            StoredTask {
                id: "node-a@0003".into(),
                bytes: task_bytes(3, "dist", "work"),
            },
        ]);
    backend_b
        .api
        .write()
        .await
        .task_queue
        .tasks
        .get_mut(&ID("dist", "work"))
        .unwrap()
        .extend([
            StoredTask {
                id: "node-b@0002".into(),
                bytes: task_bytes(2, "dist", "work"),
            },
            StoredTask {
                id: "node-b@0004".into(),
                bytes: task_bytes(4, "dist", "work"),
            },
        ]);

    let mut client = engine_client(&proxy.addr).await?;
    let page0 = client
        .get_tasks(admin_request(proto::TaskPageRequest {
            namespace: "dist".into(),
            task: "work".into(),
            page: 0,
            page_size: 2,
            state: proto::TaskState::Queued as i32,
        })?)
        .await?
        .into_inner();
    let page1 = client
        .get_tasks(admin_request(proto::TaskPageRequest {
            namespace: "dist".into(),
            task: "work".into(),
            page: 1,
            page_size: 2,
            state: proto::TaskState::Queued as i32,
        })?)
        .await?
        .into_inner();

    assert_eq!(
        page0
            .tasks
            .iter()
            .map(|task| task.id.clone())
            .collect::<Vec<_>>(),
        vec!["node-a@0001".to_string(), "node-a@0003".to_string()]
    );
    assert_eq!(
        page1
            .tasks
            .iter()
            .map(|task| task.id.clone())
            .collect::<Vec<_>>(),
        vec!["node-b@0002".to_string(), "node-b@0004".to_string()]
    );

    backend_a.shutdown().await;
    backend_b.shutdown().await;
    proxy.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn cluster_membership_expires_after_heartbeats_stop() -> Result<(), BoxError> {
    let proxy = spawn_proxy().await?;
    let backend_a = spawn_backend("node-a", &["auth", "control"], &proxy.addr).await?;
    let backend_b = spawn_backend("node-b", &["gpu"], &proxy.addr).await?;

    wait_for_node_count(&proxy.addr, 2).await?;
    backend_b.shutdown().await;
    wait_for_node_count(&proxy.addr, 1).await?;

    let mut client = cluster_client(&proxy.addr).await?;
    let nodes = client
        .list_nodes(cluster_request(proto::Empty {})?)
        .await?
        .into_inner()
        .nodes;
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].node_id, "node-a");

    backend_a.shutdown().await;
    proxy.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn reregister_replaces_prior_session_id() -> Result<(), BoxError> {
    let proxy = spawn_proxy().await?;
    let mut client = cluster_client(&proxy.addr).await?;

    let first = client
        .register_node(cluster_request(proto::NodeRegisterRequest {
            node_id: "node-z".into(),
            advertise_addr: "http://127.0.0.1:59999".into(),
            tags: vec!["gpu".into()],
            tasks: vec!["dist:work".into()],
        })?)
        .await?
        .into_inner();
    let second = client
        .register_node(cluster_request(proto::NodeRegisterRequest {
            node_id: "node-z".into(),
            advertise_addr: "http://127.0.0.1:59999".into(),
            tags: vec!["gpu".into()],
            tasks: vec!["dist:work".into()],
        })?)
        .await?
        .into_inner();

    assert_ne!(first.session_id, second.session_id);

    let old_session = client
        .heartbeat(cluster_request(proto::NodeHeartbeat {
            node_id: "node-z".into(),
            session_id: first.session_id,
        })?)
        .await
        .unwrap_err();
    assert_eq!(old_session.code(), Code::PermissionDenied);

    client
        .heartbeat(cluster_request(proto::NodeHeartbeat {
            node_id: "node-z".into(),
            session_id: second.session_id,
        })?)
        .await?;

    proxy.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn legacy_ids_use_broadcast_fallback_for_publish_and_delete() -> Result<(), BoxError> {
    let proxy = spawn_proxy().await?;
    let backend_a = spawn_backend("node-a", &["auth", "control"], &proxy.addr).await?;
    let backend_b = spawn_backend("node-b", &["gpu"], &proxy.addr).await?;

    backend_b
        .api
        .write()
        .await
        .executing_tasks
        .tasks
        .get_mut(&ID("dist", "work"))
        .unwrap()
        .push(StoredExecutingTask {
            bytes: task_bytes(11, "dist", "work"),
            id: "legacy-1".into(),
            user_id: "worker-1".into(),
            given_at: Utc::now(),
        });
    backend_b
        .api
        .write()
        .await
        .task_queue
        .tasks
        .get_mut(&ID("dist", "work"))
        .unwrap()
        .push(StoredTask {
            id: "legacy-2".into(),
            bytes: task_bytes(12, "dist", "work"),
        });

    let mut client = engine_client(&proxy.addr).await?;
    client
        .publish_task(worker_request(proto::Task {
            id: "legacy-1".into(),
            task_id: "dist:work".into(),
            task_payload: task_bytes(111, "dist", "work"),
            payload: Vec::new(),
        })?)
        .await?;
    client
        .delete_task(admin_request(proto::TaskSelector {
            state: proto::TaskState::Queued as i32,
            namespace: "dist".into(),
            task: "work".into(),
            id: "legacy-2".into(),
        })?)
        .await?;

    let solved = backend_b
        .api
        .read()
        .await
        .solved_tasks
        .tasks
        .get(&ID("dist", "work"))
        .unwrap()
        .len();
    let queued = backend_b
        .api
        .read()
        .await
        .task_queue
        .tasks
        .get(&ID("dist", "work"))
        .unwrap()
        .len();
    assert_eq!(solved, 1);
    assert_eq!(queued, 0);

    backend_a.shutdown().await;
    backend_b.shutdown().await;
    proxy.shutdown().await;
    Ok(())
}
