# Security policy

## Supported versions

Only the latest minor release of OpenQVD is supported with
security fixes.

| Version | Supported |
| ------- | --------- |
| 1.2.x   | Yes       |
| < 1.2   | No        |

## Reporting a vulnerability

Please report security vulnerabilities privately via
[GitHub Security Advisories](https://github.com/Sigilweaver/OpenQVD/security/advisories/new).

Do **not** open a public issue for security reports. We will
acknowledge within 7 days and aim to publish a fix or mitigation
within 30 days for confirmed issues.

## Scope

In scope:

- Memory-safety bugs in the QVD parser or writer.
- Path traversal, decompression bombs, or arbitrary file write
  triggered by a malformed `.qvd` file.
- Crashes that can be triggered by an attacker-supplied file.
- Supply-chain integrity issues affecting published crates or
  wheels.

Out of scope:

- Incorrect parsing of malformed-but-non-crashing files. Please
  open a normal issue with a reproducer.
- Vulnerabilities in third-party crates: please report those
  upstream.

## Disclosure

Coordinated disclosure is preferred. Once a fix is released, the
advisory will be made public and credited to the reporter unless
they request anonymity.
