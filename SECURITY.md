# Security policy

## Supported versions

Security fixes are made against the latest published release. Public Works is
currently below `1.0`, so fixes may include API, checkpoint, or schema changes.
Applications with durable work in progress should review the relevant crate
release notes before upgrading.

| Version | Supported |
| --- | --- |
| Latest published release | Yes |
| Older releases | No |

## Reporting a vulnerability

Use GitHub's private vulnerability reporting form:

<https://github.com/claytercek/publicworks/security/advisories/new>

Do not open a public issue for a suspected vulnerability. Include the affected
crate and version, expected impact, reproduction steps or a proof of concept, and
any known mitigations. Reports involving persisted data should also describe the
schema or checkpoint version involved.

Please allow time to investigate and prepare a fix before publishing details. The
maintainer will use the private advisory to coordinate questions, remediation,
and disclosure.

## Security boundaries

Public Works runs host-provided task handlers, tools, extensions, and model
providers in process. It does not sandbox them or treat them as untrusted code.
The runtime also does not provide authorization, credential management,
multi-process ownership locks, or exactly-once external effects.

A report is still useful when implementation behavior contradicts these documented
boundaries, exposes data outside them, bypasses an explicit policy check, or
corrupts durable state.
