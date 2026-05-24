# Releasing

This document is for maintainers.

## Release Flow

1. Merge the release-ready changes to `main`.
2. Create a tagged release from `main`.
3. Let GitHub Actions build the release artifacts.
4. Let the Homebrew tap workflow update `rtk-ai/homebrew-tap`.

## Required Secret

Set `HOMEBREW_TAP_TOKEN` in GitHub Actions.

The token needs write access to `rtk-ai/homebrew-tap`.
A classic PAT with `repo` and `workflow` scopes is the simplest option.

## Notes

- The Homebrew update is maintainer-only release automation.
- Contributors do not need to manage tap publishing.
- Prereleases should not update the tap unless you explicitly opt in.
