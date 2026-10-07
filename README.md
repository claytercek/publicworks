# Public Works

Public Works is our own Rust runtime project, modeled on Pi Durable. The working
slice is a **Session transaction layer for conversations, immutable entries, and
durable tasks and submissions**, with in-memory and SQLite storage. A host-polled `Harness` opens and reconciles
all supported foreground task trees, then atomically reserves every eligible task
and polls their local handler futures concurrently after explicit `resume`.
Durable waits, held outcomes, cancellation cascades, task observation, and orderly
join-before-storage-close are included. The lower-level explicit-root `TaskRunner`
remains available. A provider-neutral agent layer adds durable model/tool turns
using host-supplied callbacks. An opt-in OpenAI Responses package provides asynchronous networking
without adding a provider or executor dependency to the core libraries.

## Try the persistence demo

With a Rust toolchain supporting edition 2024, run these commands from the repo
root. Use a new database path to get the example IDs:

```sh
cargo run -p publicworks-cli -- create /tmp/publicworks-demo.db
# {"commitSeq":1,"conversationId":2}
cargo run -p publicworks-cli -- append /tmp/publicworks-demo.db 2 "hello"
# {"commitSeq":2,"conversationId":2,"entryId":3}
cargo run -p publicworks-cli -- show /tmp/publicworks-demo.db 2
# Inspect or withdraw a receipt created by an embedding agent host:
cargo run -p publicworks-cli -- submission /tmp/publicworks-demo.db SUBMISSION_ID
cargo run -p publicworks-cli -- abort-submission /tmp/publicworks-demo.db SUBMISSION_ID 2
```

Each command opens and closes the database. `create` adds a conversation;
`append` stores a `publicworks.text` entry with `data: {"text":"hello"}`;
`show` prints visible entries newest-first. `submission` prints one committed
receipt. `abort-submission` withdraws a queued receipt and optionally verifies
its conversation; it cannot cancel placed work. Successful commands print JSON.
Errors go to stderr with a nonzero exit status. Commands other than `create`
require an existing database and do not create one accidentally.

The CLI uses Session transactions and always polls orderly shutdown. It is a
transcript and maintenance utility, not a chat agent. Text is one application
payload, not the core entry model. It cannot generically submit or resume agent
work because that requires the host's exact model, extension/tool installation,
task registry, and credential policy. Root conversation ID 1 is reserved from
allocation but is not automatically created; ordinary IDs start at 2.

## Libraries

- `runtime/` — `publicworks-runtime`: records, object-safe async-compatible
  `Storage`, `Session`/`Tx`, host-polled `SessionDriver`, fork-aware reads, and
  task creation/ownership validation, startup normalization, a host-polled
  `Harness`/`HarnessDriver`, the lower-level `TaskRunner`/`TaskDriver`, and public
  `MemoryStorage`. Uses serde/serde_json and small futures primitives;
  no SQL, Tokio, or executor dependency.
- `agent/` — `publicworks-agent`: immutable model/tool installation, atomic turn
  admission, pinned requests, fork-aware context projection, ordered tool children,
  and conservative interrupted-effect recovery. No provider or executor dependency.
- `providers/openai/` — `publicworks-provider-openai`: opt-in, non-streaming
  OpenAI Responses text/function-call adapter using reqwest/rustls. Requires a
  Tokio host; excluded from the workspace's default members.
- `storage/sqlite/` — `publicworks-storage-sqlite`: `SqliteStorage::open(path)`;
  bundled SQLite through rusqlite, no external server.
- `cli/` — `publicworks-cli`: the `publicworks` binary, composing the public
  Session interface and SQLite with futures-lite.
- `perf/` — `publicworks-perf`: non-default deterministic performance/resource
  workloads for Memory, SQLite, Session, Harness, context, and agent paths.

The in-memory and SQLite storage adapters return boxed futures but **block while
polled**. The opt-in HTTP provider uses asynchronous network I/O. SQLite uses WAL,
NORMAL synchronization, and one transaction per batch. This is an embedded,
single-logical-owner foundation, not a multiwriter runtime or a power-loss
persistence guarantee. The current schema is Public Works' own typed, indexed v5.
It uses separate record tables plus a global ID registry, bounded SQL reads, and
leased durable ID ranges. There are no existing users or schema migrations: incompatible databases reject
without being deleted or rewritten. Use a new path or manually recreate a
disposable development database when the format changes. During this pre-user
phase, backward compatibility is not promised for the SQLite schema, persisted
agent/task checkpoints, or task-definition versions. Exact definition versions
remain required; neither startup path migrates them. Add migration machinery only
after retained user data creates that requirement. Atomic write rollback remains
required; it prevents partial batches and is separate from migrating old formats. Payloads use finite
native serde_json numbers (full i64/u64 and round-trippable f64), not arbitrary
precision decimal text, and have a maximum nesting depth of 64. See the contract
for how model/edit and task state/outcome/memo wrappers count toward that limit. Opaque JSON decoding remains
correct when an embedding application enables additional serde_json features.

## Host a Session

The host must poll the driver concurrently with command futures, including
shutdown. This example uses futures-lite; an existing local executor works too.
The runtime and its Storage interface do not require `Send`.

```rust
use futures_lite::future::{block_on, zip};
use publicworks_runtime::{EntryDraft, MemoryStorage, Session};

block_on(async {
    let (session, driver) = Session::new(MemoryStorage::new());
    let command = async {
        let result = session.commit(|tx| Box::pin(async move {
            let conversation = tx.create_conversation().await?;
            tx.append_entry(conversation.id, EntryDraft::new("example")).await
        })).await;
        // Always close, including when the command fails.
        let closed = session.close().await;
        closed?;
        result
    };
    let (result, ()) = zip(command, driver).await;
    let receipt = result.unwrap();
    assert_eq!(receipt.seq.unwrap().get(), 1);
});
```

`commit` admits its owned callback synchronously. Dropping its returned waiter,
including before its first poll, does not cancel the admitted transaction. The
driver owns callback execution and storage settlement. Read-only and empty
transactions skip Storage.commit and return `seq: None`. Returned records are
detached values; public table reads after any mutation attempt reject.

Submission transactions support queued input/write creation, placement,
first-writer-wins settlement, and atomic queued withdrawal. Dedicated conversation
state stores the run marker, ordered inbox, and opaque agent configuration. State
updates and submission changes compose privately in either staging order; final
assembly validates their references before persistence. Request-ID lookup is
conversation-scoped and can return an existing receipt without writing. Agent
admission builds on these operations; see the
implemented Tx reference.

`close()` immediately seals admission, drains admitted transactions, and calls
Storage.close once. Repeated calls share the result; dropping a close waiter does
not stop cleanup. Dropping the last Session handle also requests drain and close.
**Dropping the driver is different:** waiters get `DriverStopped`, and in-flight
persistence or asynchronous cleanup is no longer guaranteed. Never await a
Session commit or close from inside its own callback; use Tx operations there.

Only an explicit no-effect storage rejection leaves a failed commit reusable.
Any other commit error or storage panic poisons the Session; it must be closed.
Callback errors/panics discard staged writes without poisoning. Started Tx
operations are drained even when their method waiters are abandoned; a callback
that returns with pending operations fails rather than committing. There is no
public commit stream or view subscription yet, so no subscriber queue to grow.
See the design contract for the boundary
between inherited Pi behavior and Rust driver/cancellation mapping.

## Schedule all ready task trees with a Harness

`Harness::open(storage, registry)` returns an opening waiter and one
`HarnessDriver`. Poll the driver while awaiting opening and for the full lifetime
of the returned `Harness`. Opening scans the task graph, atomically normalizes
interrupted `running` records to `pending`, and schedules code-free reconciliation.
It never invokes a definition or handler.

The scheduler remains paused until `harness.resume()` or a progress-enabling
wait (`Submission::wait` or `wait_for_idle`). Resume is idempotent and
permanently enables progress. Each drain reconciles committed records and changes
**all** currently eligible tasks from `pending` to `running` in one Session
transaction. Handler futures start only after that batch is acknowledged. They are
then polled concurrently by the same local driver; the runtime spawns no executor
work and requires neither `Send` nor Tokio.

Use `harness.commit` for application transactions, `wait_task` for a committed
terminal record, and `inspect` for a detached view of live tasks and scheduler
blocks. Dropping a task waiter abandons only that observation. Replacing the
immutable `TaskRegistry` snapshot wakes blocked work; every reservation retains
one stable snapshot. Missing or non-exact definitions leave normal work pending,
while abort-marked work without an exact definition becomes orphaned only after
owned work drains. A known no-effect reservation rejection parks until a later
wakeup or explicit resume rather than busy-spinning. Uncertain persistence or an
unrecoverable invocation settlement error seals the Harness, rejects observers,
and is reported by close; handler code is not replayed speculatively.

Close through `harness.close()` while continuing to poll `HarnessDriver`. Close
seals public admission, signals and joins every active invocation, drains admitted
Session work, and only then closes Storage. A noncooperative handler can keep close
pending. Dropping the driver is forfeiture: late runtime access is fenced, but an
in-flight persistence operation or external effect may have an uncertain outcome.

Ownership traversal follows both task-owner and conversation-owner edges; fork
ancestry remains independent. Background tasks are independently scheduled anchors:
ordinary parent cancellation, held outcomes, and idle observation stop at their
boundary. `harness.conversation(id)` reacquires a stateless orchestration handle.
Use its `wait_for_idle()` and `abort(options)` methods for scoped observation and
cancellation, or `harness.wait_for_idle()` across ownerless conversations. Idle
waits enable progress and observe committed liveness, not merely runnable handlers.
Set `ConversationAbortOptions { background: true }` only when cancellation should
cross background boundaries and wait for the reached snapshot. See the
ownership lifecycle contract.

## Observe and withdraw durable submissions

`harness.submission(id).await?` reacquires an optional, cloneable `Submission`.
`submission.id()` returns its identity; `status().await?` (or `read().await?`)
returns a detached committed `SubmissionRecord`. Lookup and reads do not resume
scheduling. The handle retains its Harness, not a cached receipt.

`submission.wait()` enables progress and observes a terminal (`done` or
`unanswered`) committed receipt. The state check and waiter registration run on
one Session line, so settlement cannot fall between them. Multiple waiters are
independent; dropping one, even before its first poll, cancels only observation.
Only final assembled submission changes acknowledged by Storage resolve waits.
Close rejects unresolved observers; dropping the driver reports `DriverStopped`.

`submission.abort().await?` (or `withdraw()`) atomically settles a queued receipt
as unanswered/aborted and removes its inbox item. It returns `Aborted`,
`AlreadyPlaced`, `Settled`, or `NotFound`; it neither cancels a placed run nor
enables scheduler progress. `harness.withdraw_submission(id, conversation_filter)`
also supports missing IDs and optional conversation filtering. Conversation
handles provide scoped `submission(id)` and `withdraw_submission(id)` methods.

`agent.submit(&harness, conversation, text, config, options)` admits a durable
input and enables progress. Matching request IDs return the original receipt
before validation or busy policy. Busy inputs support reject, steer, and follow-up
(the default). `agent.write(&harness, conversation, draft, options)` admits a
passive write without enabling progress; busy writes queue. Dropping an admission
observer does not cancel its transaction.

`agent.configure_queues` persists independent one/all modes for steers and
follow-ups. Post-tools places writes and steers; final places writes and follow-ups,
settles current inputs, and creates a successor atomically. Explicit run abort and
model failure preserve queued later work. Task-owned conversations are supported.

Phase 4 is complete. Runtime assembly settles any still-placed inputs and clears
the matching run marker when the final task candidate becomes terminal, including
faults, panics, no-progress/malformed checkpoints, and missing-definition aborts.
Fallback is unanswered/`aborted` for an aborted outcome and `faulted` otherwise;
a specific settlement already staged by the agent wins. Cleanup does not inspect
task results or call agent code. Held outcomes retain placed inputs until terminal,
and a successor marker is never cleared for the old task.

Conversation abort bulk-withdraws queued inputs in its reached owned conversation
scopes, including queued-only conversations. Task abort/failure cascades withdraw
queued inputs in owned descendant conversations. Queued writes remain, ordinary
direct run abort preserves its own later queue, and background boundaries are
crossed only by an explicitly background-inclusive conversation abort. Reopening
repairs terminal markers and reapplies withdrawal beneath terminal failed/aborted
owners without invoking handlers. See the
submission contract
for settlement, scope, and persistence rules.

## Create and execute one task tree explicitly

`TaskDefinition::new(kind, version, initial, phases)` combines a reusable
synchronous initial-checkpoint function with a `BTreeMap<String, PhaseHandler>`.
Definitions are immutable and cloneable. `TaskRegistry::new(definitions)` rejects
duplicate kinds. Neither creating a definition nor registering it runs code.

Inside a Session callback, `tx.create_task(definition, input, options)` persists a
pending task and returns a detached `TaskRecord`. `TaskOptions` requires explicit
conversation or task ownership. Task-owned children inherit their owner's
conversation and cannot be background. Conversation-owned tasks may be background,
and task-owned conversations extend the owner's ordinary scope. Use `tx.task` and
`tx.scan_tasks` for committed reads before mutations.

For execution, attach one runner with
`TaskRunner::attach(&session, registry)`, then call `runner.run(task.id)`.
The host must poll **both** the SessionDriver and TaskDriver. The TaskDriver owns
admitted run requests and handler futures; SessionDriver remains the only Storage
owner. A handler awaiting external work does not occupy the Session queue.
Dropping a run waiter does not cancel its request.

`run(root)` drives one ordinary ownership scope with only one handler active at a
time. The scope follows task ownership and task-owned conversations. A background
conversation-owned task is an independent root and is not driven as part of its
former foreground scope. The requested root must be a scheduling anchor;
`run(child)` is not a second hosting mode. Pending, waiting, and completing roots
can be driven, but unrecovered running records require `Session::open_recovered`
first. Terminal roots return `Blocked(NotPending)`.

Normal execution requires an exact definition version. A blocked root does not
prevent eligible children from running. `RunResult::Terminal` is a durable root
receipt; `Suspended(TaskRecord)` is the durable root at quiescence, when no
supported work can execute. `TaskRunner` itself has no global scheduler, timer,
or automatic retry: explicitly run the root again when an observed external
dependency changes, or use a Harness for Harness-wide progress.

A handler receives its detached task and a `TaskRuntime`. It calls
`runtime.commit(|tx, current| ...)` to atomically write entries and return an
optional `TaskUpdate::Checkpoint`, `Wait { checkpoint, on, policy }`, `Complete`,
`Fail`, or `Abort { reason, result }`. Entries receive task
attribution automatically. Each successful phase must change its structural
checkpoint, commit a wait, or commit an outcome; returning without durable progress faults the
task. External effects are not part of the storage transaction and are not
exactly-once.

Run the [leaf host example](runtime/examples/task_execution.rs) or the
[tree example](runtime/examples/task_tree.rs):

```sh
cargo run -p publicworks-runtime --example task_execution
cargo run -p publicworks-runtime --example task_tree
```

On shutdown, await `runner.close()` while polling both drivers, then await
`session.close()`. Runner close signals `runtime.cancelled()`, interrupts queued
unreserved requests, joins the cooperative handler, and drains runner-admitted
mutations. A queued mutation whose callback has not started rejects at the closing
gate; draining does not promise it will apply. A noncooperative handler can keep
runner close pending. Session close signals the runner but does not join external
handler code. Dropping TaskDriver forfeits settlement and fences old contexts.
See the execution contract for details.

## Run a durable agent turn

Install a local `Model` implementation and `Tool` callbacks with `Agent::new` from
`publicworks-agent`, open a Harness with `agent.definitions()`, and call
`agent.submit` while polling the Harness driver. Idle admission atomically creates
the user entry, placed submission, generation task, and live-run marker. Busy
inputs follow the requested reject/steer/follow-up policy. Await the returned
submission's `wait()` to observe its durable settlement.

```sh
cargo run -p publicworks-agent --example agent_turn
```

The example uses a fake provider and tool; no credentials or network are needed.
Requests pin model identity, instructions, offered tool versions, and projected
messages. Calls execute one at a time after durable intent. Tools default to
`ReplayPolicy::Unsafe`: an interrupted tool records an uncertain-effect error,
then the turn continues under its model-round limit. Opt in locally with
`tool.with_replay_policy(ReplayPolicy::Safe)`. Recovery re-executes only if the
stored policy and currently selected same-name/version tool are both safe, using
stored effective arguments without rerunning `beforeTool`. See the
replay contract. A model can request a
similar action with a new call ID, so approvals and external idempotency remain
application responsibilities.
Cancellation does not undo effects. See the
agent contract for the API and limits.

### Select and publish extensions

For named tool bundles, install `Extension` values in an `AgentRegistry`, then
construct `Agent::with_registry(model, registry.snapshot(), host_default)`.
`None` for the host default selects all installed extensions in installation
order. `Agent::new` remains the single-bundle convenience constructor.

Persist per-conversation policy with
`agent.configure_extensions(&harness, conversation, ExtensionConfig { selection,
config }).await?`. Selection supports `Default`, ordered `Exact`, and `AddRemove`.
The config map holds JSON settings by extension name. Missing names survive
restart and become effective when installed. This update and `configure_queues`
preserve each other's fields; neither starts generation or runs callbacks.

After installing, replacing, or uninstalling a bundle, publish its snapshot:

```rust,ignore
agent = agent.publish_snapshot(&harness, registry.snapshot(), other_definitions)?;
```

Pass every non-agent task definition in `other_definitions`; this replaces the
Harness definition map in one publication and wakes scheduling. Old phases keep
their captured code. Later compatible phase boundaries use the new definitions.
Request preparation pins selected tool declarations, while each tool call checks
current selection against its captured phase snapshot. An accepted implementation
stays pinned through its effect and result. Add provider-neutral callbacks with
`Extension::with_hooks(LifecycleHooks { .. })`: the implemented sites are
`before_request`, `after_response`, `before_tool`, `after_tool`, and `after_tools`.
Hooks run in selected-extension order outside storage transactions. `HookContext`
provides cooperative cancellation and invocation-fenced, first-writer-wins memos;
it does not expose raw transaction or task-transition access. Ordinary hook errors
are recorded, and `before_tool` errors additionally fail closed as a blocked call.

Callbacks are never persisted and must be reinstalled on restart. Public Works
maintains the extension API and a small set of tested recipes, not a first-party
extension catalog. Copy a recipe into your host or depend on a community crate.
Extensions compile into the host; there is no dynamic Rust plugin ABI. Example
helper APIs carry no compatibility promise.

Package-specific recipes live beside their owning crate; top-level examples are
reserved for applications that compose several packages. To try a selected
`before_tool` policy with a deterministic local model:

```sh
cargo run -p publicworks-agent --example tool_policy
cargo test -p publicworks-agent --example tool_policy
```

Copy and adapt `host_policy` in
[`agent/examples/tool_policy.rs`](agent/examples/tool_policy.rs), then install
and select it as shown there. Replace the example's decision with your own
application policy. A block becomes an immediate `tool_blocked` result before
durable effect intent. Omitting or deselecting the hook leaves tools unguarded.
This is not a manual approval queue, authorization boundary, or sandbox. Hook
semantics belong to the API and core tests, not to this recipe. See the
optional integration contract.

Prompt sections, wrappers, filters, yield hooks, and compaction hooks remain
deferred until the agent has corresponding provider-neutral request or lifecycle
models. See the extension contract
for the configuration wire format, definition-version change, hook composition,
and remaining scope.

## Opt in to OpenAI Responses

`publicworks-provider-openai` implements the same `Model` interface with a real
asynchronous HTTP client. Use `OpenAiResponses::new(api_key)` or
`OpenAiResponses::with_config(api_key, Config { .. })` and install it in the agent.
Poll the host on a Tokio runtime with I/O and time enabled; a current-thread
runtime is enough. Runtime and agent themselves remain executor-neutral.

The live example uses `gpt-4.1-mini`. It is not run by the test suite and requires
an explicit credential; running it sends data to OpenAI and can incur charges:

```sh
# Set OPENAI_API_KEY securely in your host environment first.
cargo run -p publicworks-provider-openai --example openai_turn
```

Credentials belong in the host environment/memory, never durable turn
configuration. The default endpoint is `https://api.openai.com/v1/responses`,
with a 120-second timeout and an 8 MiB response cap. Configuration accepts a
validated full HTTPS URL (numeric loopback HTTP is allowed for fixtures).
Redirects, retries, and ambient proxies are disabled.

The adapter supports text and local function calls, not every OpenAI model:
reasoning, streaming, images, OAuth, and built-in remote tools are unsupported.
Unknown semantic output, refusals, or incomplete output fail rather than produce
executable calls. Local tools still execute sequentially.

Model inputs, outputs, tool data, partial text, and usage can be persisted; this
adapter is not a secret scrubber. Cancellation and timeouts cannot undo server
work or billing. Recovery may replay a request and bill twice. `store:false`
is sent on every request but is not a zero-retention guarantee. See the
provider contract for the full boundary.

No credentials or real API calls are needed for provider tests:

```sh
cargo test -p publicworks-provider-openai --locked
cargo test -p publicworks-provider-openai --locked --features serde_json/arbitrary_precision
```

## Cancel a task durably

`runner.abort(id)` immediately admits a Session-owned command. Its `AbortWaiter`
returns `Result<AbortResult, RunError>`: `Marked`, `Terminal`, or
`Blocked(BlockReason)`. `Marked` means the mutation was acknowledged and the
target's normal invocation observed on the Session line has ended. It does **not** mean
cleanup finished or the task has an aborted outcome. Missing targets error;
unsupported scopes are untouched. Abort can target the root or a descendant;
it marks that subtree, not unrelated siblings. Terminal targets and unchanged
repeated marks consume no commit sequence. Missing definitions or exact-version
mismatches orphan a target only after its owned descendants have drained.

Affected normal handlers are signalled only after durable acknowledgement,
even when a child is waiting for cancellation while its parent is waiting on it.
Cleanup runs bottom-up with fresh, initially unsignalled abort invocations. Repeated requests do
not cancel cleanup. The default handler commits `Aborted`; use
`definition.with_abort_handler(handler)` to install a `PhaseHandler`-shaped custom
handler. It must commit `Abort`, `Complete`, or `Fail`. Returning without an outcome
faults, even after checkpoint or entry writes. Abort dispatch does not validate
the normal checkpoint's phase.

With the lower-level `TaskRunner`, an **inactive** marked task needs an explicit
`runner.run(root)` to execute cleanup. A resumed Harness instead notices the
committed mark and schedules eligible cleanup automatically. Never await your own
`runner.abort(id)` from its current normal handler: it joins that invocation and
would deadlock. Enqueuing without awaiting is allowed.

Dropping an abort waiter never cancels the admitted command. Dropping TaskDriver
also leaves admitted abort mutations owned by SessionDriver, but forfeits handler
settlement. In contrast, `runner.close()` requests cooperative shutdown without
persisting abort intent; reopening can resume normal work. Close does not start
fresh cleanup. Keep both drivers polling to drain admitted mutations. External
cleanup must tolerate replay after a crash. See the
cancellation contract.

## Reopen interrupted tasks

`Session::open_recovered(storage)` returns `(CommitWaiter<Session>, SessionDriver)`.
Poll the driver concurrently with the opening waiter, then take the usable Session
from the successful receipt's `value`. Its `seq` is present when startup normalizes
running tasks or repairs terminal submission state. All running pages are read before one atomic
replacement batch; checkpoints, input, version, owner, flags, and memos survive.
Plain `Session::new` remains unchanged.

Opening runs no task code. Unknown kinds/versions are preserved, and waiting,
completing, and terminal task states are not changed by this lower-level API.
Stale terminal run markers and their placed inputs are repaired, and queued inputs
under terminal failed/aborted owners are withdrawn atomically with normalization.
After opening, explicitly attach a runner and request each desired root run. Prefer
`Harness::open` when the host wants Harness-wide reconciliation, reservation, and
automatic progress after one resume. Neither path performs definition or schema
migration. Dropping the Session opening waiter still leaves normalization and
close owned by the driver. See the task persistence contract.

## Development

```sh
cargo fmt --all -- --check
cargo test --workspace --all-features --locked
cargo test --workspace --all-features --locked --features serde_json/arbitrary_precision
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo clippy --workspace --all-targets --all-features --locked --features serde_json/arbitrary_precision -- -D warnings
```

These commands are the documented development checks, not a statement that the
release gate has passed. The unchecked release gates
also require package checks, fake/local-provider examples, and Markdown-link
checks. This repository does not currently define one checked-in release command
or Markdown-link-checker configuration. Do not run the live OpenAI example as a
release smoke test without explicit credential opt-in.

The shared conformance checks exercise the public `Storage` seam against both
adapters. SQLite tests also cover reopen, allocator continuity, rollback, invalid
schemas, corrupt reads, and Session transactions across reopen. Session tests
cover FIFO settlement, failed callbacks, fork validation, poison classification,
panics, abandoned operations, waiter/driver drops, close, task ownership and
replacement validation, and startup normalization. Execution tests cover phase
progress and error precedence, attribution, drive-lifetime tree guards, invocation fencing,
rejection versus uncertain commits, cooperative shutdown, and SQLite reopen.
Harness release-gate tests cover paused opening, resume and reconciliation wakeups,
known rejection versus uncertain persistence, abandoned observers, reentrant wakes,
noncooperative close, driver forfeiture, and SQLite recovery after uncertain commits.
Tree tests cover joins, held outcomes, bottom-up cancellation, acknowledgement
races, scope validation, quiescence, and SQLite recovery. Persisted entry, task,
submission, and conversation-state JSON checks run under both the workspace's normal
serde_json features and `serde_json/arbitrary_precision` feature unification. SQLite
rejects incompatible schemas rather than migrating them. The executable
smoke test runs create/append/show in
separate processes. See the contract for what
these checks do and do not establish. Packages remain unpublished; `Cargo.lock`
is retained for reproducible builds.
