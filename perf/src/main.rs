use futures_lite::future::{block_on, zip};
use futures_util::future::join_all;
use publicworks_agent::*;
use publicworks_runtime::*;
use publicworks_storage_sqlite::SqliteStorage;
use serde_json::{Value, json};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

struct CountingAllocator;
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, old: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, old, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

type AnyResult<T> = Result<T, Box<dyn Error>>;

#[derive(Clone, Copy)]
struct Profile {
    name: &'static str,
    entries: usize,
    payload_bytes: usize,
    exact_reads: usize,
    roots: usize,
    rounds: usize,
    reopen_cycles: usize,
}
impl Profile {
    fn named(name: &str) -> Option<Self> {
        Some(match name {
            "quick" => Self {
                name: "quick",
                entries: 64,
                payload_bytes: 256,
                exact_reads: 8,
                roots: 8,
                rounds: 2,
                reopen_cycles: 2,
            },
            "baseline" => Self {
                name: "baseline",
                entries: 512,
                payload_bytes: 2 * 1024,
                exact_reads: 32,
                roots: 64,
                rounds: 4,
                reopen_cycles: 8,
            },
            "stress" => Self {
                name: "stress",
                entries: 2_048,
                payload_bytes: 16 * 1024,
                exact_reads: 64,
                roots: 256,
                rounds: 8,
                reopen_cycles: 16,
            },
            _ => return None,
        })
    }
}

#[derive(Clone, Copy)]
struct AllocationMark {
    count: u64,
    bytes: u64,
}
fn allocation_mark() -> AllocationMark {
    AllocationMark {
        count: ALLOCATIONS.load(Ordering::Relaxed),
        bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
    }
}
fn rss_kib(field: &str) -> Option<u64> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find(|line| line.starts_with(field))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}
fn file_size(path: &Path) -> u64 {
    fs::metadata(path).map_or(0, |metadata| metadata.len())
}
fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{suffix}", path.display()))
}
#[allow(clippy::too_many_arguments)]
fn emit(
    profile: Profile,
    adapter: &str,
    workload: &str,
    operations: usize,
    elapsed: Duration,
    before: AllocationMark,
    db: u64,
    wal: u64,
    checksum: u64,
) {
    let after = allocation_mark();
    let nanos = elapsed.as_nanos();
    let per_second = if elapsed.is_zero() {
        0.0
    } else {
        operations as f64 / elapsed.as_secs_f64()
    };
    println!(
        "{}\t{}\t{}\t{}\t{}\t{:.2}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        profile.name,
        adapter,
        workload,
        operations,
        nanos,
        per_second,
        after.count - before.count,
        after.bytes - before.bytes,
        rss_kib("VmRSS:").unwrap_or(0),
        rss_kib("VmHWM:").unwrap_or(0),
        db,
        wal,
        checksum
    );
}
fn measured<T>(
    profile: Profile,
    adapter: &str,
    workload: &str,
    operations: usize,
    db_path: Option<&Path>,
    work: impl FnOnce() -> AnyResult<(T, u64)>,
) -> AnyResult<T> {
    let before = allocation_mark();
    let started = Instant::now();
    let (value, checksum) = work()?;
    finish_measurement(
        profile, adapter, workload, operations, db_path, before, started, checksum,
    );
    Ok(value)
}
async fn measured_async<T>(
    profile: Profile,
    adapter: &str,
    workload: &str,
    operations: usize,
    db_path: Option<&Path>,
    work: impl Future<Output = AnyResult<(T, u64)>>,
) -> AnyResult<T> {
    let before = allocation_mark();
    let started = Instant::now();
    let (value, checksum) = work.await?;
    finish_measurement(
        profile, adapter, workload, operations, db_path, before, started, checksum,
    );
    Ok(value)
}
#[allow(clippy::too_many_arguments)]
fn finish_measurement(
    profile: Profile,
    adapter: &str,
    workload: &str,
    operations: usize,
    db_path: Option<&Path>,
    before: AllocationMark,
    started: Instant,
    checksum: u64,
) {
    let elapsed = started.elapsed();
    let (db, wal) = db_path.map_or((0, 0), |path| {
        (file_size(path), file_size(&sidecar(path, "-wal")))
    });
    emit(
        profile, adapter, workload, operations, elapsed, before, db, wal, checksum,
    );
}

fn id(value: u64) -> Id {
    Id::new(value).expect("fixture ID")
}
fn text(size: usize, salt: usize) -> String {
    let prefix = format!("{salt:016x}:");
    let mut value = String::with_capacity(size);
    while value.len() < size {
        value.push_str(&prefix);
    }
    value.truncate(size);
    value
}
fn user_entry(number: u64, conversation: Id, bytes: usize) -> EntryRecord {
    let mut entry = EntryRecord::new(id(number), conversation, "perf.user");
    entry.model = Some(vec![encode_message(&ModelMessage::User {
        text: text(bytes, number as usize),
    })]);
    entry
}
fn fixture_writes(profile: Profile) -> (Vec<StorageWrite>, Id, Id, Id) {
    let root = id(2);
    let mut writes = vec![StorageWrite::Conversation(ConversationRecord {
        id: root,
        parent: None,
        owner: None,
    })];
    for offset in 0..profile.entries as u64 {
        writes.push(StorageWrite::Entry(user_entry(
            3 + offset,
            root,
            profile.payload_bytes,
        )));
    }
    let last_root = id(2 + profile.entries as u64);
    let fork = id(3 + profile.entries as u64);
    writes.push(StorageWrite::Conversation(ConversationRecord {
        id: fork,
        parent: Some(ParentLink {
            conversation_id: root,
            at: id(3 + profile.entries as u64 / 2),
        }),
        owner: None,
    }));
    let child_entry = id(4 + profile.entries as u64);
    writes.push(StorageWrite::Entry(user_entry(
        child_entry.get(),
        fork,
        profile.payload_bytes,
    )));
    let large_task = id(5 + profile.entries as u64);
    writes.push(StorageWrite::Task(TaskRecord {
        id: large_task,
        conversation_id: root,
        kind: "perf.large-payload".into(),
        version: 1,
        input: json!({"payload": text(profile.payload_bytes * 4, 7)}),
        owner: None,
        background: false,
        abort_requested: false,
        state: TaskState::Pending {
            checkpoint: json!({"phase":"run","payload":text(profile.payload_bytes * 4, 9)}),
        },
        memos: None,
    }));
    (writes, last_root, fork, large_task)
}

async fn storage_workload<S: Storage>(
    mut storage: S,
    profile: Profile,
    adapter: &str,
    db: Option<&Path>,
) -> AnyResult<()> {
    let (writes, exact, fork, task) = fixture_writes(profile);
    measured(
        profile,
        adapter,
        "session_batch_equivalent_commit",
        writes.len(),
        db,
        || {
            let seq = block_on(storage.commit(writes))?;
            Ok(((), seq.get()))
        },
    )?;
    measured(
        profile,
        adapter,
        "exact_reads",
        profile.exact_reads * 2,
        db,
        || {
            let mut checksum = 0;
            for _ in 0..profile.exact_reads {
                checksum += block_on(storage.entry(exact))?.is_some() as u64;
                checksum += block_on(storage.task(task))?.is_some() as u64;
            }
            Ok(((), checksum))
        },
    )?;
    measured(
        profile,
        adapter,
        "paginated_visible_scan",
        profile.entries / 64 + 1,
        db,
        || {
            let mut cursor = None;
            let mut count = 0_u64;
            loop {
                let page = block_on(storage.scan_entries(EntryQuery::new(fork), 64, cursor))?;
                count += page.items.len() as u64;
                cursor = page.next;
                if cursor.is_none() {
                    break;
                }
            }
            Ok(((), count))
        },
    )?;
    let next = id(6 + profile.entries as u64);
    measured(
        profile,
        adapter,
        "populated_store_small_commit",
        1,
        db,
        || {
            let seq = block_on(storage.commit(vec![StorageWrite::Entry(user_entry(
                next.get(),
                id(2),
                32,
            ))]))?;
            Ok(((), seq.get()))
        },
    )?;
    block_on(storage.close())?;
    Ok(())
}

async fn context_workload<S: Storage + 'static>(
    mut storage: S,
    profile: Profile,
    adapter: &str,
    db: Option<&Path>,
) -> AnyResult<()> {
    let (writes, _, fork, _) = fixture_writes(profile);
    storage.commit(writes).await?;
    let (session, driver) = Session::new(storage);
    let command = async move {
        measured_async(profile, adapter, "session_commit_small", 1, db, async {
            let receipt = session
                .commit(move |tx| {
                    Box::pin(async move {
                        let mut draft = EntryDraft::new("perf.user");
                        draft.model = Some(vec![encode_message(&ModelMessage::User {
                            text: "incremental".into(),
                        })]);
                        tx.append_entry(id(2), draft).await
                    })
                })
                .await?;
            Ok(((), receipt.seq.map_or(0, Seq::get)))
        })
        .await?;
        measured_async(
            profile,
            adapter,
            "context_projection",
            profile.entries,
            db,
            async {
                let projection = session
                    .commit(move |tx| {
                        Box::pin(async move { project_context(tx, fork, None).await })
                    })
                    .await?
                    .value;
                Ok(((), projection.messages.len() as u64))
            },
        )
        .await?;
        session.close().await?;
        Ok::<_, Box<dyn Error>>(())
    };
    zip(command, driver).await.0
}

fn completing_definition(calls: Rc<Cell<usize>>) -> TaskDefinition {
    let handler: PhaseHandler = Rc::new(move |_, runtime| {
        let calls = calls.clone();
        Box::pin(async move {
            calls.set(calls.get() + 1);
            runtime
                .commit(|_, _| Box::pin(async { Ok(Some(TaskUpdate::Complete(json!(null)))) }))
                .await
                .map_err(|error| TaskOutcomeError {
                    message: error.to_string(),
                    detail: None,
                })?;
            Ok(())
        })
    });
    TaskDefinition::new(
        "perf.ready-root",
        1,
        |_| Ok(json!({"phase":"run"})),
        BTreeMap::from([("run".into(), handler)]),
    )
}
async fn ready_roots<S: Storage + 'static>(
    storage: S,
    profile: Profile,
    adapter: &str,
    db: Option<&Path>,
) -> AnyResult<()> {
    let calls = Rc::new(Cell::new(0));
    let definition = completing_definition(calls.clone());
    let (opening, driver) = Harness::open(storage, TaskRegistry::new([definition.clone()])?);
    let command = async move {
        let harness = opening.await?;
        let tasks = harness
            .commit(move |tx| {
                Box::pin(async move {
                    let mut tasks = Vec::new();
                    for index in 0..profile.roots {
                        let conversation = tx.create_conversation().await?;
                        tasks.push(
                            tx.create_task(
                                definition.clone(),
                                json!({"index":index,"payload":text(profile.payload_bytes, index)}),
                                TaskOptions {
                                    ownership: TaskOwnership::Conversation,
                                    conversation_id: Some(conversation.id),
                                    background: false,
                                },
                            )
                            .await?,
                        );
                    }
                    Ok(tasks)
                })
            })
            .await?
            .value;
        measured_async(
            profile,
            adapter,
            "task_reservation_settlement",
            profile.roots,
            db,
            async {
                let waits = tasks
                    .iter()
                    .map(|task| harness.wait_task(task.id))
                    .collect::<Vec<_>>();
                harness.resume()?;
                let settled = join_all(waits).await;
                let terminal = settled
                    .into_iter()
                    .filter_map(Result::ok)
                    .filter(|task| task.status() == TaskStatus::Terminal)
                    .count();
                Ok(((), terminal as u64))
            },
        )
        .await?;
        assert_eq!(calls.get(), profile.roots);
        harness.close().await?;
        Ok::<_, Box<dyn Error>>(())
    };
    zip(command, driver).await.0
}

#[derive(Clone)]
struct RoundModel {
    rounds: usize,
    calls: Rc<Cell<usize>>,
    request_bytes: Rc<Cell<usize>>,
}
impl Model for RoundModel {
    fn complete(&self, request: ModelRequest, _: Cancellation) -> ModelFuture {
        let rounds = self.rounds;
        let calls = self.calls.clone();
        let request_bytes = self.request_bytes.clone();
        Box::pin(async move {
            calls.set(calls.get() + 1);
            let encoded = request
                .messages
                .iter()
                .map(encode_message)
                .collect::<Vec<Value>>();
            request_bytes.set(request_bytes.get() + serde_json::to_vec(&encoded).unwrap().len());
            let completed = request
                .messages
                .iter()
                .filter(|message| matches!(message, ModelMessage::ToolResult { .. }))
                .count();
            if completed < rounds {
                Ok(ModelResponse {
                    message: ModelMessage::Assistant {
                        text: String::new(),
                        tool_calls: vec![ToolCall {
                            id: format!("call-{completed}"),
                            name: "echo".into(),
                            arguments: json!({"round":completed}),
                        }],
                    },
                    finish_reason: FinishReason::ToolCalls,
                    usage: None,
                })
            } else {
                Ok(ModelResponse {
                    message: ModelMessage::Assistant {
                        text: "done".into(),
                        tool_calls: vec![],
                    },
                    finish_reason: FinishReason::Stop,
                    usage: None,
                })
            }
        })
    }
}
fn echo_tool() -> Tool {
    Tool::new(
        ToolDeclaration {
            name: "echo".into(),
            version: 1,
            description: "Return the round number".into(),
            parameters: json!({"type":"object"}),
        },
        |_| Ok(()),
        |call, _| {
            Box::pin(async move {
                Ok(ToolResult {
                    content: call.arguments["round"].to_string(),
                    is_error: false,
                    usage: None,
                })
            })
        },
    )
}
async fn agent_rounds<S: Storage + 'static>(
    storage: S,
    profile: Profile,
    adapter: &str,
    db: Option<&Path>,
) -> AnyResult<()> {
    let calls = Rc::new(Cell::new(0));
    let request_bytes = Rc::new(Cell::new(0));
    let agent = Agent::new(
        RoundModel {
            rounds: profile.rounds,
            calls: calls.clone(),
            request_bytes: request_bytes.clone(),
        },
        vec![echo_tool()],
    )?;
    let (opening, driver) = Harness::open(storage, TaskRegistry::new(agent.definitions())?);
    let command = async move {
        let harness = opening.await?;
        let conversation = harness
            .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
            .await?
            .value;
        measured_async(
            profile,
            adapter,
            "agent_rounds_pinned_requests",
            profile.rounds + 1,
            db,
            async {
                let submission = agent
                    .submit(
                        &harness,
                        conversation.id,
                        text(profile.payload_bytes, 42),
                        TurnConfig {
                            model: "perf-model".into(),
                            instructions: text(profile.payload_bytes / 2, 43),
                            max_model_rounds: profile.rounds as u64 + 1,
                        },
                        SubmitOptions::default(),
                    )
                    .await?;
                let settlement = submission.wait().await?;
                Ok(((), settlement.id.get() ^ request_bytes.get() as u64))
            },
        )
        .await?;
        assert_eq!(calls.get(), profile.rounds + 1);
        harness.close().await?;
        Ok::<_, Box<dyn Error>>(())
    };
    zip(command, driver).await.0
}

fn db_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "publicworks-perf-{}-{label}.sqlite",
        std::process::id()
    ))
}
fn remove_db(path: &Path) {
    for candidate in [
        path.to_path_buf(),
        sidecar(path, "-wal"),
        sidecar(path, "-shm"),
    ] {
        let _ = fs::remove_file(candidate);
    }
}
fn cleanup_db(path: &Path) {
    if std::env::var_os("PUBLICWORKS_PERF_KEEP_DB").is_none() {
        remove_db(path);
    }
}

async fn sqlite_reopen_cycles(profile: Profile) -> AnyResult<()> {
    let path = db_path("cycles");
    remove_db(&path);
    let mut next = 2_u64;
    for cycle in 0..profile.reopen_cycles {
        let mut storage = measured(profile, "sqlite", "sqlite_reopen", 1, Some(&path), || {
            let storage = SqliteStorage::open(&path)?;
            Ok((storage, cycle as u64))
        })?;
        let conversation = id(next);
        next += 1;
        let entry = id(next);
        next += 1;
        let writes = vec![
            StorageWrite::Conversation(ConversationRecord {
                id: conversation,
                parent: None,
                owner: None,
            }),
            StorageWrite::Entry(user_entry(entry.get(), conversation, profile.payload_bytes)),
        ];
        measured(
            profile,
            "sqlite",
            "sqlite_cycle_write",
            writes.len(),
            Some(&path),
            || {
                let seq = block_on(storage.commit(writes))?;
                Ok(((), seq.get()))
            },
        )?;
        measured(
            profile,
            "sqlite",
            "sqlite_cycle_read",
            1,
            Some(&path),
            || {
                let found = block_on(storage.entry(entry))?.is_some() as u64;
                Ok(((), found))
            },
        )?;
        storage.close().await?;
    }
    cleanup_db(&path);
    Ok(())
}

fn sqlite_case(
    label: &str,
    run: impl FnOnce(SqliteStorage, &Path) -> AnyResult<()>,
) -> AnyResult<()> {
    let path = db_path(label);
    remove_db(&path);
    let storage = SqliteStorage::open(&path)?;
    let result = run(storage, &path);
    cleanup_db(&path);
    result
}

fn main() -> AnyResult<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let profile_name = match args.as_slice() {
        [] => "quick",
        [flag, value] if flag == "--profile" => value,
        _ => return Err("usage: publicworks-perf [--profile quick|baseline|stress]".into()),
    };
    let profile = Profile::named(profile_name).ok_or("unknown profile")?;
    println!(
        "profile\tadapter\tworkload\toperations\telapsed_ns\toperations_per_second\tallocations\tallocated_bytes\trss_kib\tpeak_rss_kib\tdb_bytes\twal_bytes\tchecksum"
    );
    block_on(async {
        storage_workload(MemoryStorage::new(), profile, "memory", None).await?;
        sqlite_case("storage", |storage, path| {
            block_on(storage_workload(storage, profile, "sqlite", Some(path)))
        })?;
        context_workload(MemoryStorage::new(), profile, "memory", None).await?;
        sqlite_case("context", |storage, path| {
            block_on(context_workload(storage, profile, "sqlite", Some(path)))
        })?;
        ready_roots(MemoryStorage::new(), profile, "memory", None).await?;
        sqlite_case("roots", |storage, path| {
            block_on(ready_roots(storage, profile, "sqlite", Some(path)))
        })?;
        agent_rounds(MemoryStorage::new(), profile, "memory", None).await?;
        sqlite_case("agent", |storage, path| {
            block_on(agent_rounds(storage, profile, "sqlite", Some(path)))
        })?;
        sqlite_reopen_cycles(profile).await?;
        Ok::<_, Box<dyn Error>>(())
    })
}
