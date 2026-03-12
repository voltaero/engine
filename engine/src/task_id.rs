use enginelib::Identifier;
use tonic::Status;

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

pub fn parse_task_key(task_id: &str) -> Result<Identifier, Status> {
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

    Ok((namespace.to_string(), task.to_string()))
}

pub fn parse_task_key_string(task_id: &str) -> Result<String, Status> {
    let (namespace, task) = parse_task_key(task_id)?;
    Ok(format!("{namespace}:{task}"))
}
