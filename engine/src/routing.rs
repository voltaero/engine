use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};

use crate::proxy_config::{ProxyConfigToml, RouteRuleToml};

#[derive(Debug, Clone)]
pub struct RouteRequirement {
    pub require_tags: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RouteRule {
    pub namespace_pattern: String,
    pub task_pattern: String,
    pub requirement: RouteRequirement,
}

impl RouteRule {
    pub fn from_toml(rule: RouteRuleToml) -> Result<Self, String> {
        let Some((namespace_pattern, task_pattern)) = rule.r#match.split_once(':') else {
            return Err(format!(
                "invalid proxy rule match '{}', expected namespace:task",
                rule.r#match
            ));
        };

        Ok(Self {
            namespace_pattern: namespace_pattern.to_string(),
            task_pattern: task_pattern.to_string(),
            requirement: RouteRequirement {
                require_tags: rule.require_tags,
            },
        })
    }

    pub fn matches_task(&self, task_key: &str) -> bool {
        let Some((namespace, task)) = task_key.split_once(':') else {
            return false;
        };

        matches_pattern(&self.namespace_pattern, namespace)
            && matches_pattern(&self.task_pattern, task)
    }
}

fn matches_pattern(pattern: &str, value: &str) -> bool {
    pattern == "*" || pattern == value
}

#[derive(Debug)]
pub struct NodeState {
    pub node_id: String,
    pub advertise_addr: String,
    pub tags: Vec<String>,
    pub tasks: Vec<String>,
    pub session_id: String,
    pub channel: Channel,
    pub last_seen_unix: AtomicU64,
    pub in_flight_create: AtomicU64,
    tag_set: BTreeSet<String>,
    task_set: BTreeSet<String>,
}

impl NodeState {
    pub fn new(
        node_id: String,
        advertise_addr: String,
        tags: Vec<String>,
        tasks: Vec<String>,
        session_id: String,
        now_unix: u64,
    ) -> Result<Self, String> {
        let channel = Endpoint::from_shared(advertise_addr.clone())
            .map_err(|err| format!("invalid advertise_addr '{advertise_addr}': {err}"))?
            .connect_lazy();

        Ok(Self {
            node_id,
            advertise_addr,
            tag_set: tags.iter().cloned().collect(),
            task_set: tasks.iter().cloned().collect(),
            tags,
            tasks,
            session_id,
            channel,
            last_seen_unix: AtomicU64::new(now_unix),
            in_flight_create: AtomicU64::new(0),
        })
    }

    pub fn has_task(&self, task_key: &str) -> bool {
        self.task_set.contains(task_key)
    }

    pub fn has_tags(&self, tags: &[String]) -> bool {
        tags.iter().all(|tag| self.tag_set.contains(tag))
    }

    pub fn last_seen_unix(&self) -> u64 {
        self.last_seen_unix.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct ProxyState {
    pub config: ProxyConfigToml,
    pub rules: Vec<RouteRule>,
    pub nodes: RwLock<HashMap<String, Arc<NodeState>>>,
}

impl ProxyState {
    pub fn new(config: ProxyConfigToml) -> Result<Self, String> {
        let rules = if config.rules.is_empty() {
            vec![RouteRule::from_toml(RouteRuleToml {
                r#match: "*:*".into(),
                require_tags: Vec::new(),
            })?]
        } else {
            let mut rules = Vec::with_capacity(config.rules.len());
            for rule in config.rules.iter().cloned() {
                rules.push(RouteRule::from_toml(rule)?);
            }
            rules
        };

        Ok(Self {
            config,
            rules,
            nodes: RwLock::new(HashMap::new()),
        })
    }

    pub async fn upsert_node(&self, node: NodeState) -> Arc<NodeState> {
        let node = Arc::new(node);
        self.nodes
            .write()
            .await
            .insert(node.node_id.clone(), node.clone());
        node
    }

    pub async fn healthy_nodes(&self) -> Vec<Arc<NodeState>> {
        let mut nodes: Vec<_> = self.nodes.read().await.values().cloned().collect();
        nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        nodes
    }

    pub async fn candidate_nodes_for_task(&self, task_key: &str) -> Vec<Arc<NodeState>> {
        let required_tags = self
            .rules
            .iter()
            .find(|rule| rule.matches_task(task_key))
            .map(|rule| rule.requirement.require_tags.clone())
            .unwrap_or_default();

        self.healthy_nodes()
            .await
            .into_iter()
            .filter(|node| node.has_task(task_key) && node.has_tags(&required_tags))
            .collect()
    }

    pub async fn preferred_nodes_by_tag(&self, tag: &str) -> Vec<Arc<NodeState>> {
        let nodes = self.healthy_nodes().await;
        let mut preferred = Vec::new();
        let mut fallback = Vec::new();

        for node in nodes {
            if node.has_tags(&[tag.to_string()]) {
                preferred.push(node);
            } else {
                fallback.push(node);
            }
        }

        preferred.extend(fallback);
        preferred
    }

    pub async fn node_by_id(&self, node_id: &str) -> Option<Arc<NodeState>> {
        self.nodes.read().await.get(node_id).cloned()
    }

    pub async fn reap_stale_nodes(&self, now_unix: u64) {
        let ttl_seconds = self.config.node_ttl_seconds;
        self.nodes
            .write()
            .await
            .retain(|_, node| now_unix.saturating_sub(node.last_seen_unix()) <= ttl_seconds);
    }
}

pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
