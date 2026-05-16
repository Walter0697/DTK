# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- add TOML, YAML, and XAML command-output support to `dtk exec`
- add optional config `format` override for structured payload parsing
- add bundled Cargo.lock-style TOML, Python manifest TOML, XAML ResourceDictionary, and Kubernetes YAML sample configs to `dtk install-dummy`
- add bundled CSV inventory export sample config and parser support to `dtk install-dummy`
- add bundled INI plugin registry sample config and parser support to `dtk install-dummy`

## [0.0.2](https://github.com/Walter0697/DTK/compare/v0.0.1...v0.0.2) - 2026-05-15

### Added

- add DTK config recommendations and config commands
- rename telemetry command to session
- add telemetry ticket sessions
- add telemetry-backed gain analytics

### Fixed

- fix retrieve recommendation tracking

### Other

- add config list to top-level help
- format DTK config changes
- Trim prompt flow config-location wording
- Clarify DTK recommendation follow-up commands
- Fix DTK config-relative allowlist filtering
- refine DTK config recommendations
- Remove completed gain roadmap item
- Clarify gain issues help
- Clarify fallback behavior in docs
- Fix fallback accounting
- Add gain issues view
- Add usage fallback tracking

## [0.0.1](https://github.com/Walter0697/DTK/releases/tag/v0.0.1) - 2026-05-12

### Other

- initial DTK repo setup
