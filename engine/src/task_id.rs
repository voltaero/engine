use enginelib::Identifier;
use tonic::Status;

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
