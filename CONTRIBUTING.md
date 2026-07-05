# Contributing

Thanks for your interest in OpenQVD. Issues, pull requests, and
clean-room corpus contributions are welcome.

## Development setup

```sh
# Rust
cargo build --workspace
cargo test --workspace

# Python bindings (editable)
cd crates/openqvd-py
maturin develop --release --features pyo3/extension-module
pytest tests/
```

## Contributing code (pull requests)

PRs are welcome for changes of any size, including large or breaking ones -
there's no requirement to open an issue first. That said, for larger changes
you may want to open an issue before writing code, especially if you're
unsure whether it fits the project's direction: a large PR that conflicts
with the roadmap can still be rejected even if the code itself is solid, and
an issue is a cheap way to check alignment before investing the time.

For any PR:

- Scope it to one logical change.
- `cargo fmt --all` clean.
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
- `cargo test --workspace` passes.
- For Python changes: `pytest tests/` passes.
- New parser/writer features must come with a `.qvd` fixture and a
  round-trip test.
- User-facing changes update the README and `SPEC.md` if relevant, and
  add a `[Unreleased]` note to [CHANGELOG.md](CHANGELOG.md).
- Prefer [Conventional Commits](https://www.conventionalcommits.org/)
  (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`).
- Code is ASCII only and `#![forbid(unsafe_code)]`, except
  `openqvd-py`, which opts in as required by pyo3 proc-macros.

## Vendor software and clean-room policy

OpenQVD is a clean-room implementation derived solely from binary analysis
of a public corpus. **Do not contribute code derived from any existing QVD
parser** (Qlik's own implementation, GPL or non-GPL third-party readers,
decompiled binaries, leaked documentation). All contributions must be your
original work or derived solely from public corpus inspection. Do not run,
depend on, or validate your implementation against Qlik's own tools, or
anything that reads the format through Qlik's own libraries - not in CI,
not in tests, not in local development.

**Pull requests that were written or verified with the help of proprietary
vendor software will not be accepted**, regardless of code quality, since
accepting them would compromise the project's clean-room provenance. If
you've found a bug this way, or you'd simply rather not write the fix
yourself, please open an issue instead. Describe the symptom on the input
that triggers it - what's wrong, and on what file - without pasting vendor
tool output, vendor source, or values you learned by running vendor
software. We'll investigate and fix it from independent analysis. Detailed
issue reports are genuinely useful and will be acted on.

## Security

Please report security vulnerabilities privately via GitHub Security
Advisories - see [SECURITY.md](SECURITY.md). Do not open public issues
for vulnerabilities.

## DCO

By submitting a contribution you certify that you have the right
to submit the work under the project license (Apache-2.0) and
agree to the
[Developer Certificate of Origin](https://developercertificate.org/).

## License

Code: [Apache-2.0](LICENSE).
Specification (`SPEC.md`): CC-BY-SA-4.0.
