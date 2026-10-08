# Performance workloads

This package is a deterministic workload runner, not a release-gating benchmark. It
uses public runtime APIs, a fake local model, a process-wide counting allocator,
and Linux `/proc` RSS data when available.

Run from the workspace root with an optimized build:

```sh
devenv shell -- cargo run --release -p publicworks-perf -- --profile quick
devenv shell -- cargo run --release -p publicworks-perf -- --profile baseline
devenv shell -- cargo run --release -p publicworks-perf -- --profile stress
```

Output is TSV so runs can be diffed or imported into a spreadsheet. Each measured
row reports elapsed time, operation throughput, allocation calls and allocated
bytes, current/peak resident memory, and database/WAL sizes when applicable.
Run one profile per process: allocator and `VmHWM` counters are process-wide.

Profiles use fixed payloads and counts. `quick` validates the harness;
`baseline` is the documented representative workload; `stress` deliberately
amplifies payload and cardinality. Each SQLite workload uses its own temporary
directory; dropping it removes the database and sidecars, including on failure.
Set `PUBLICWORKS_PERF_KEEP_DB=1` to retain those directories instead. Their paths
are printed to stderr without changing the TSV output.
