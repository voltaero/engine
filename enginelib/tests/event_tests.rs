use enginelib::{Registry, api::EngineAPI, events::ID, task::Verifiable};
use macros::Verifiable;
use std::sync::Arc;
use tracing_test::traced_test;

#[derive(Clone, Debug, macros::Event)]
#[event(namespace = "test", name = "test_event", cancellable)]
struct TestEvent {
    pub value: i32,
    pub cancelled: bool,
}

#[macros::event_handler(namespace = "test", name = "test_event")]
fn increment_test_event(event: &mut TestEvent) {
    event.value += 1;
}

#[derive(Clone, Debug, macros::Event)]
#[event(namespace = "test", name = "stateful_event")]
struct StatefulEvent {
    pub value: u32,
}

#[macros::event_handler(namespace = "test", name = "stateful_event", ctx = 7u32)]
fn add_ctx_to_event(event: &mut StatefulEvent, ctx: &u32) {
    event.value += *ctx;
}

#[traced_test]
#[test]
fn id() {
    assert!(ID("namespace", "id") == ID("namespace", "id"))
}

#[traced_test]
#[test]
fn test_event_registration_and_handling() {
    let mut api = EngineAPI::test_default();

    let mut test_event = TestEvent {
        value: 0,
        cancelled: false,
    };

    enginelib::event::register_inventory_handlers(&mut api);
    api.event_bus.fire(&mut test_event);
    assert_eq!(test_event.value, 1);
    drop(api.db);
}

#[traced_test]
#[test]
fn test_stateful_event_auto_registration() {
    let mut api = EngineAPI::test_default();

    enginelib::event::register_inventory_handlers(&mut api);

    let mut event = StatefulEvent { value: 0 };
    api.event_bus.fire(&mut event);
    assert_eq!(event.value, 7);
    drop(api.db);
}

#[traced_test]
#[test]
fn test_task_registration() {
    use enginelib::task::Task;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, Verifiable)]
    struct TestTask {
        pub value: i32,
        pub id: (String, String),
    }

    impl Task for TestTask {
        fn to_toml(&self) -> String {
            "".into()
        }
        fn get_id(&self) -> (String, String) {
            self.id.clone()
        }
        fn clone_box(&self) -> Box<dyn Task> {
            Box::new(self.clone())
        }
        fn run_cpu(&mut self) {
            self.value += 1;
        }
        fn to_bytes(&self) -> Vec<u8> {
            postcard::to_allocvec(self).unwrap()
        }
        fn from_bytes(&self, bytes: &[u8]) -> Box<dyn Task> {
            Box::new(postcard::from_bytes::<TestTask>(bytes).unwrap())
        }
        fn from_toml(&self, d: String) -> Box<dyn Task> {
            return Box::new(self.clone());
        }
    }
    let mut api = EngineAPI::test_default();
    let task_id = ID("test", "test_task");

    // Register the task type
    api.task_registry.register(
        Arc::new(TestTask {
            value: 0,
            id: task_id.clone(),
        }),
        task_id.clone(),
    );

    // Verify it was registered
    assert!(api.task_registry.tasks.contains_key(&task_id));
    drop(api.db);
}

#[traced_test]
#[test]
fn test_task_execution() {
    use enginelib::task::{Runner, Task};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, Verifiable)]
    struct TestTask {
        pub value: i32,
        pub id: (String, String),
    }

    impl Task for TestTask {
        fn to_toml(&self) -> String {
            "".into()
        }
        fn from_toml(&self, d: String) -> Box<dyn Task> {
            return Box::new(self.clone());
        }
        fn get_id(&self) -> (String, String) {
            self.id.clone()
        }
        fn clone_box(&self) -> Box<dyn Task> {
            Box::new(self.clone())
        }
        fn run_cpu(&mut self) {
            self.value += 1;
        }
        fn to_bytes(&self) -> Vec<u8> {
            postcard::to_allocvec(self).unwrap()
        }
        fn from_bytes(&self, bytes: &[u8]) -> Box<dyn Task> {
            Box::new(postcard::from_bytes::<TestTask>(bytes).unwrap())
        }
    }

    // Test task execution directly
    let mut task = TestTask {
        value: 0,
        id: ID("test", "test_task"),
    };

    task.run(Some(Runner::CPU));
    assert_eq!(task.value, 1);
}

#[traced_test]
#[test]
fn test_task_serialization() {
    use enginelib::task::{StoredTask, Task};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, Verifiable)]
    struct TestTask {
        pub value: i32,
        pub id: (String, String),
    }

    impl Task for TestTask {
        fn to_toml(&self) -> String {
            "".into()
        }
        fn from_toml(&self, d: String) -> Box<dyn Task> {
            return Box::new(self.clone());
        }
        fn get_id(&self) -> (String, String) {
            self.id.clone()
        }
        fn clone_box(&self) -> Box<dyn Task> {
            Box::new(self.clone())
        }
        fn run_cpu(&mut self) {
            self.value += 1;
        }
        fn to_bytes(&self) -> Vec<u8> {
            postcard::to_allocvec(self).unwrap()
        }
        fn from_bytes(&self, bytes: &[u8]) -> Box<dyn Task> {
            Box::new(postcard::from_bytes::<TestTask>(bytes).unwrap())
        }
    }

    let task = TestTask {
        value: 42,
        id: ID("test", "test_task"),
    };

    // Test serialization and deserialization
    let serialized = task.to_bytes();
    let stored_task = StoredTask {
        bytes: serialized,
        id: "id".into(),
    };

    // Deserialize
    let deserialized_task: TestTask = postcard::from_bytes(&stored_task.bytes).unwrap();
    assert_eq!(deserialized_task.value, 42);

    // Test the from_bytes function
    let recreated_task = task.from_bytes(&stored_task.bytes);
    // We need a way to check the value inside the recreated task
    // Since we can't directly access the value, we'll serialize it again and deserialize manually
    let bytes = recreated_task.to_bytes();
    let final_task: TestTask = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(final_task.value, 42);
}
