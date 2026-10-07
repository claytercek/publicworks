# Public Works

Public Works is an experimental Rust runtime for durable conversations, tasks,
and model/tool turns. Hosts provide storage, task definitions, model providers,
and tools; Public Works persists progress and recovers interrupted work.

The project is at `0.1`. Persisted schemas, task checkpoints, and public APIs may
change between releases. Public Works does not provide exactly-once external I/O,
forced cancellation, authorization, or sandboxing.

## Crates

| Package | Purpose |
| --- | --- |
| [`publicworks-runtime`](runtime/) | Executor-neutral sessions, durable task trees, submissions, and an in-memory storage adapter |
| [`publicworks-storage-sqlite`](storage/sqlite/) | Bundled SQLite storage for a single logical owner |
| [`publicworks-agent`](agent/) | Provider-neutral durable model/tool turns and extension hooks |
| [`publicworks-provider-openai`](providers/openai/) | Optional non-streaming OpenAI Responses adapter |

The `publicworks-cli` persistence utility and `publicworks-perf` workload runner
are source-only workspace packages.

## Try it

Run a complete agent turn with a local fake model and tool:

```sh
devenv shell -- cargo run -p publicworks-agent --example agent_turn
```

Or try the SQLite persistence utility:

```sh
devenv shell -- cargo run -p publicworks-cli -- create /tmp/publicworks.db
devenv shell -- cargo run -p publicworks-cli -- append /tmp/publicworks.db 2 "hello"
devenv shell -- cargo run -p publicworks-cli -- show /tmp/publicworks.db 2
```

Each runtime entry point returns a driver future. The host must keep that driver
polled while commands run and through orderly shutdown. The runtime does not
spawn an executor and its futures need not be `Send`. The built-in memory and
SQLite adapters perform synchronous work when polled; the OpenAI adapter uses
asynchronous I/O and requires a Tokio host.

Package READMEs and rustdoc contain the setup, lifecycle, recovery, and safety
notes for each crate. Runnable examples live beside the crate that owns the API:

- `runtime/examples/` covers task execution, task trees, cancellation, and memos.
- `agent/examples/` covers a durable turn and host-defined tool policy.
- `providers/openai/examples/` contains the opt-in live provider example.

## Development

Use the checked-in development environment:

```sh
devenv shell -- cargo fmt --all -- --check
devenv shell -- cargo test --workspace --all-features --locked
devenv shell -- cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

The complete offline release check also builds rustdoc, runs both supported
`serde_json` feature configurations, validates examples and local Markdown
links, and packages every publishable crate:

```sh
devenv shell -- cargo fetch --locked
devenv shell -- cargo fmt --all -- --check
devenv shell -- cargo test --workspace --all-features --locked
devenv shell -- cargo test --workspace --all-features --locked --features serde_json/arbitrary_precision
devenv shell -- cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
devenv shell -- env RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
devenv shell -- cargo package --workspace --locked
```

The Checks workflow contains the shared verification job. On `main`, Release
calls Checks instead of starting a second verification pipeline. Its release
jobs depend on that result and are gated by the `RELEASE_PR_ENABLED` and
`RELEASE_PUBLISH_ENABLED` repository variables. Verification has read-only
permissions; release jobs receive only their required write permissions. A newer
main-branch commit does not interrupt an in-progress release. To run verification
and release automation manually, dispatch Release on `main`.

The publish job uses crates.io trusted publishing through OIDC; the first
publication of a new crate must still be performed deliberately by a maintainer
before configuring its trusted publisher.

## License

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option.
