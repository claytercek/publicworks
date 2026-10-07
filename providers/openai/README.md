# publicworks-provider-openai

An opt-in OpenAI Responses adapter for Public Works agents.

Supports non-streaming text and function-call turns using reqwest/rustls. Requires a Tokio host. Live requests require explicit credentials.

See the [repository documentation](https://github.com/claytercek/publicworks)
for host setup, examples, recovery semantics, and current limitations.

This is an early 0.1 release. Durable schema compatibility is not promised.
There is no exactly-once external I/O, forced cancellation, authorization, or
sandboxing guarantee.

## License

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE),
at your option. This includes the examples; retain applicable license notices
when copying them.
