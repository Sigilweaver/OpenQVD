# Releasing OpenQVD

Checklist for cutting a release. OpenQVD ships one crate (`openqvd`) and
one Python package (`openqvd`, PyO3 bindings) from a single `vX.Y.Z` tag.

## 1. Confirm CI and audit are green

Publishing is gated on a tag push, and GitHub Actions cannot make one
workflow file `needs:` a job defined in another workflow file, so this has
to be checked before the tag exists rather than inside `publish.yml`.

Run:

```sh
./scripts/check-release-ready.sh
```

against the commit you intend to tag (defaults to `HEAD`; pass a ref or
SHA to check something else). It fails closed: it exits non-zero and
prints why if `ci.yml` or `audit.yml` hasn't run for that commit, is still
running, or didn't succeed. Do not tag until it prints
"Release ready" and exits 0.

A red `audit.yml` isn't necessarily a blocker on its own (see
`.github/workflows/audit.yml` - some advisories are deliberately
`--ignore`d against unfixable pinned transitive deps and tracked in an
issue), but if it's failing for a *new* reason, understand why before
releasing.

## 2. Bump the version

Version lives in three places and they must match:

- `Cargo.toml`: `[workspace.package] version = "X.Y.Z"` (both crates,
  `openqvd` and `openqvd-py`, inherit it via `version.workspace = true`).
- `crates/openqvd-py/pyproject.toml`: `[project] version = "X.Y.Z"`.
- `CITATION.cff`: `version: "X.Y.Z"` and `date-released: "YYYY-MM-DD"`.

Run `cargo build --workspace` (or `cargo check --workspace`) afterwards so
`Cargo.lock` picks up the new `openqvd`/`openqvd-py` versions.

If this is a major version bump, also check
`crates/openqvd-py/Cargo.toml`'s `openqvd = { version = "1.2", path =
"../openqvd", ... }` dependency line - Cargo's default caret matching
means a minor/patch bump doesn't need it touched, but a major bump does.

## 3. Update CHANGELOG.md

Move the `[Unreleased]` section's contents under a new `## [X.Y.Z] -
YYYY-MM-DD` heading, following [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Leave an empty `[Unreleased]` heading in place for the next round of
changes.

## 4. Commit, tag, push

```sh
git add Cargo.toml Cargo.lock crates/openqvd-py/pyproject.toml CITATION.cff CHANGELOG.md
git commit -m "release: vX.Y.Z"
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin main
git push origin vX.Y.Z
```

The tag push triggers `.github/workflows/publish.yml`, which publishes
`openqvd` to crates.io and builds+publishes the `openqvd` Python wheels
and sdist to PyPI.

## 5. Verify

- Watch the `Publish` workflow run to completion:
  `gh run list -w publish.yml -c $(git rev-parse vX.Y.Z)`.
- Check the new version shows up on
  [crates.io/crates/openqvd](https://crates.io/crates/openqvd) and
  [pypi.org/project/openqvd](https://pypi.org/project/openqvd/).
- Sanity-check install: `pip install --upgrade openqvd` /
  `cargo add openqvd` and confirm the version.
- If the Zenodo GitHub integration is enabled, confirm a new DOI version
  was minted for the tag.
