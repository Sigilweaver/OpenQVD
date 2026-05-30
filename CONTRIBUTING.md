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

## Pull request checklist

- `cargo fmt --all` clean.
- `cargo clippy --workspace --all-targets -- -D warnings` clean.
- `cargo test --workspace` passes.
- For Python changes: `pytest tests/` passes.
- New parser/writer features must come with a `.qvd` fixture and
  a round-trip test.
- User-facing changes update the README and `SPEC.md` if
  relevant, and add a `[Unreleased]` note to
  [CHANGELOG.md](CHANGELOG.md).

## Clean-room policy

OpenQVD is a clean-room implementation derived solely from binary
analysis of a public corpus. **Do not contribute code derived
from any existing QVD parser** (Qlik's own implementation, GPL or
non-GPL third-party readers, decompiled binaries, leaked
documentation). All contributions must be your original work or
derived solely from public corpus inspection.

## DCO

By submitting a contribution you certify that you have the right
to submit the work under the project license (Apache-2.0) and
agree to the
[Developer Certificate of Origin](https://developercertificate.org/).

## License

Code: [Apache-2.0](LICENSE).
Specification (`SPEC.md`): CC-BY-SA-4.0.
