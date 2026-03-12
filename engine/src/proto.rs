// Shared gRPC/protobuf bindings for the engine crate (bins + tests).
tonic::include_proto!("engine");

pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("engine_descriptor");
