use tonic::{
    Request,
    metadata::{KeyAndValueRef, MetadataMap},
};

pub mod cluster_client;
pub mod proto;
pub mod proxy_config;
pub mod routing;
pub mod service;
pub mod task_id;

pub fn get_uid<T>(req: &Request<T>) -> String {
    req.metadata()
        .get("uid")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_default()
}

pub fn copy_metadata(source: &MetadataMap, target: &mut MetadataMap) {
    for entry in source.iter() {
        match entry {
            KeyAndValueRef::Ascii(key, value) => {
                target.insert(key, value.clone());
            }
            KeyAndValueRef::Binary(key, value) => {
                target.insert_bin(key, value.clone());
            }
        }
    }
}

pub fn get_auth<T>(req: &Request<T>) -> String {
    req.metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_default()
}
