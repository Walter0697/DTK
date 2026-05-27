# Releasing

This document is for maintainers.

## Release Flow

1. Merge the release-ready changes to `main`.
2. Create a tagged release from `main`.
3. Let GitHub Actions build the release artifacts.
4. Manually update `Walter0697/homebrew-tap` if you want the tap to point at the new release.

## Required Secret

No extra secret is required for manual tap updates.

If you later re-enable automation, set `HOMEBREW_TAP_TOKEN` in GitHub Actions and give it write access to `Walter0697/homebrew-tap`.

## Notes

- The Homebrew update is maintainer-only release work.
- Contributors do not need to manage tap publishing.
- Prereleases should not update the tap unless you explicitly opt in.
