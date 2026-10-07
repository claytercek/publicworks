# publicworks-provider-openai

An opt-in, non-streaming OpenAI Responses adapter for `publicworks-agent`.
It supports text and local function calls over reqwest/rustls. Poll model futures
inside a Tokio runtime with I/O and time enabled.

## Configure the provider

```rust,no_run
use publicworks_provider_openai::{Config, ConfigError, OpenAiResponses};
use std::time::Duration;

fn provider(api_key: String) -> Result<OpenAiResponses, ConfigError> {
    OpenAiResponses::with_config(api_key, Config {
        endpoint: "https://api.openai.com/v1/responses".into(),
        timeout: Duration::from_secs(120),
        max_response_bytes: 8 * 1024 * 1024,
    })
}
```

`OpenAiResponses::new` uses those defaults. Keep credentials in host memory;
the provider does not read environment variables. Do not put credentials in
turn configuration, instructions, tools, or other durable payloads.

The endpoint must be HTTPS, except for numeric loopback HTTP used by local tests.
Redirects, retries, and ambient proxies are disabled. Response bodies are bounded.
Error diagnostics do not retain the API key, URL, response body, or raw reqwest
error.

## Supported scope

Use a non-reasoning model that supports Responses text and function calls.
Streaming, reasoning-state replay, images, audio, OAuth, built-in remote tools,
and server-side conversation state are not supported. Unsupported semantic
output fails rather than being silently discarded.

Every request sends `store: false`, but that is not a zero-retention guarantee.
Application inputs, outputs, tool data, partial text, and usage may be persisted
by the agent. Cancellation and timeouts cannot retract a transmitted request or
prevent billing, and recovery may replay an interrupted request.

The live example requires `OPENAI_API_KEY`, sends data to OpenAI, and can incur
charges:

```sh
cargo run -p publicworks-provider-openai --example openai_turn
```

Provider tests use local fixtures and make no real API calls.

## License

Licensed under either MIT or Apache-2.0, at your option.
