# Contributing

DTK uses a PR-first workflow.

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

## Releases

Release artifacts and any future Homebrew tap should be driven from tagged releases.
