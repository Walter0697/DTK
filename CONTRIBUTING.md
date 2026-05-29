# Contributing

DTK uses a PR-first workflow.

## Ways To Contribute

Good contributions are:

- bug fixes
- parser and structured-format support improvements
- filtering and retrieval correctness fixes
- PII rule improvements
- test coverage
- documentation improvements
- install and setup improvements

For larger changes, open an issue or discussion first so the scope stays aligned with the project direction.

## Design Rules

Follow the same basic discipline that RTK uses for its own filtering layer, adapted to DTK’s structured-payload model:

- preserve correctness over aggressive shrinking
- keep the filtered surface recoverable through `_dtk.ref_id`
- keep format-specific output compatible with the original source shape
- do not block execution if a filter or parser path fails
- keep startup overhead low
- reuse the existing structured parser, filter, and retrieval layers instead of adding one-off logic

## Branch Flow

1. Create a feature branch from `main`.
2. Open a pull request.
3. Wait for CI to pass.
4. Merge only after review and checks.

## GitHub Settings To Enable

These are not enforced by the repo itself. Set them in GitHub after publishing the repository:

- protect `main`
- require pull requests before merging
- require status checks to pass
- require linear history if you want a clean release history
- optionally require code owner reviews

## Local Checks

Run these before opening a PR:

```bash
cargo fmt --check
cargo test
./install.sh
```
