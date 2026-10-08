# Contributing to Public Works

Public Works welcomes bug reports, documentation fixes, tests, and focused code
changes. Open an issue before starting a large feature or changing a public API,
persisted format, or recovery contract. This gives maintainers a chance to confirm
the scope before substantial work begins.

## Development environment

The repository uses [devenv](https://devenv.sh/) to provide Rust and the other
build tools. After cloning the repository, run commands through the checked-in
environment:

```sh
devenv shell -- cargo test --workspace --all-features --locked
```

You can also enter the shell once and run Cargo directly:

```sh
devenv shell
cargo test --workspace --all-features --locked
```

Public Works supports Rust 1.88 and newer. CI checks the minimum supported Rust
version separately from the main test suite.

## Tests and checks

Run the full local verification before opening a pull request:

```sh
devenv shell -- cargo fmt --all -- --check
devenv shell -- cargo test --workspace --all-features --locked
devenv shell -- cargo test --workspace --all-features --locked --features serde_json/arbitrary_precision
devenv shell -- cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
devenv shell -- cargo clippy --workspace --all-targets --all-features --locked --features serde_json/arbitrary_precision -- -D warnings
devenv shell -- env RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
```

Run the local examples as additional integration checks:

```sh
devenv shell -- cargo run -p publicworks-runtime --example task_abort --locked
devenv shell -- cargo run -p publicworks-runtime --example task_execution --locked
devenv shell -- cargo run -p publicworks-runtime --example task_memo --locked
devenv shell -- cargo run -p publicworks-runtime --example task_tree --locked
devenv shell -- cargo run -p publicworks-agent --example agent_turn --locked
devenv shell -- cargo run -p publicworks-agent --example tool_policy --locked
```

The OpenAI example sends real data and may incur charges. Do not run it as a
routine test.

Verify distributable archives with a fresh target directory. A separate directory
prevents an older package artifact with the same unpublished version from being
reused during verification:

```sh
package_target="$(mktemp -d)"
devenv shell -- env CARGO_TARGET_DIR="$package_target" \
  cargo package --workspace --locked
rm -rf "$package_target"
```

## Design constraints

Changes should preserve the contracts documented by the crate that owns them.
In particular:

- the runtime remains independent of a particular async executor;
- futures and storage adapters are not assumed to be `Send`;
- durable state changes remain atomic at the storage batch boundary;
- external effects are not described as exactly once;
- persisted formats and recovery behavior are documented when they change;
- provider credentials and executable callbacks are not stored in durable data.

Add regression tests for bug fixes. Changes to storage behavior should exercise
both the in-memory reference adapter and SQLite when the shared contract applies.

## Pull requests

Keep pull requests focused and explain any user-visible compatibility impact.
Update crate documentation and examples alongside API or behavior changes. Commit
messages use [Conventional Commits](https://www.conventionalcommits.org/) because
release automation derives versions and changelogs from them.

Examples:

```text
fix(storage): preserve entry cursor order
feat(agent): add request lifecycle hook
refactor(runtime)!: change checkpoint ownership
```

Use a `BREAKING CHANGE:` footer when the subject alone does not explain the
compatibility impact. Do not edit release tags or publish crates from a
contribution branch.

## License

Unless stated otherwise, contributions are licensed under either MIT or
Apache-2.0, at your option, on the same terms as the repository.
