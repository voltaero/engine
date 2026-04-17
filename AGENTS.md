# AGENTS.md

Operational guide for coding agents in this repository.

## Scope

Rust workspace for a distributed task engine with dynamic mods.

Workspace members:
- `engine` (binaries: server/client/packer)
- `enginelib` (shared runtime API, events, tasks, plugin loader)
- `enginelib/macros` (proc macros used by mods and core)

## Start here (fast orientation)

1. `Cargo.toml` (workspace)
2. `engine/src/bin/server.rs`
3. `enginelib/src/api.rs`
4. `enginelib/src/event.rs`
5. `enginelib/src/plugin.rs`
6. `engine/proto/engine.proto`

For deeper indexing details, see `AGENT_INDEX.md`.

## Build/test commands

- Build all: `cargo build --workspace`
- Test all: `cargo test --workspace`
- Run server: `cargo run -p engine --bin server`
- Run client: `cargo run -p engine --bin client`

## Key architecture facts

- gRPC service is in `engine/src/bin/server.rs`.
- Proto definitions in `engine/proto/engine.proto`; generated at build time by `engine/build.rs`.
- Engine state is centralized in `enginelib::api::EngineAPI`.
- Persistent queues are sled-backed (`engine_db` / `engine_client_db`).
- Core event system is in `enginelib/src/event.rs`; core events in `enginelib/src/events/*`.
- Mods are loaded from `./mods/*.rf` by `enginelib/src/plugin.rs`.

## Data/persistence model (important)

Sled keys:
- `tasks` -> `TaskQueue`
- `executing_tasks` -> `ExecutingTaskQueue`
- `solved_tasks` -> `SolvedTasks`

`Identifier` = `(namespace, task)`.
Task instance IDs are separate string IDs.

## Mod ABI contract (must keep)

Each `.rf` package must contain platform lib at root:
- Linux `mod.so`, macOS `mod.dylib`, Windows `mod.dll`

Exported symbols required:
- `metadata() -> LibraryMetadata`
- `run(api: &mut EngineAPI)`

`#[metadata]` and `#[module]` macros are the preferred way to provide these.

Strict compatibility check is enabled when `GE_STRICT_MODS` is set.

## Auth behavior warning

- User auth/admin auth are event-driven.
- Default `AuthEvent` handler currently allows auth (`true`).
- Admin auth checks `config.toml` token (`cgrpc_token`), with permissive behavior when unset.

Do not assume secure defaults without verifying handlers/mods.

## Change playbooks

### gRPC API change
1. Edit `engine/proto/engine.proto`
2. Update server implementation (`engine/src/bin/server.rs`)
3. Update client calls (`engine/src/bin/client.rs`)
4. Run `cargo test --workspace`

### Task lifecycle/queue change
- Touch: `engine/src/bin/server.rs`, `enginelib/src/task.rs`, `enginelib/src/api.rs`
- Validate DB sync + task transitions (queued/executing/solved)

### Event/macro change
- Runtime dispatch: `enginelib/src/event.rs`
- Macro generation: `enginelib/macros/src/lib.rs`
- Core event wrappers: `enginelib/src/events/*.rs`

### Mod loading issue
- Inspect `enginelib/src/plugin.rs`
- Cross-check `rfc/rfc1002.md`

## Quality gates before finishing

Always run:
- `cargo test --workspace`

If changing API surface or behavior, also run:
- `cargo build --workspace`

Report any warnings/errors that indicate risk (panic paths, serialization, auth flow).

## Notes for future agents

- Existing code has several warnings and `unwrap()` calls in runtime paths.
- Prefer minimal, targeted edits.
- Preserve current external behavior unless task explicitly requests behavior change.
