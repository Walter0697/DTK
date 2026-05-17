# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.4](https://github.com/Walter0697/DTK/compare/v0.0.3...v0.0.4) - 2026-05-17

### Other

- Expand PII templates and retrieve controls
- Add PII template filters
- Fix PII matching under content path
- Add PII config commands
- Expand PII replacement modes
- Add PII filtering

## [0.0.3](https://github.com/Walter0697/DTK/compare/v0.0.2...v0.0.3) - 2026-05-16

### Added

- add multi-format dummy sample support ([#6](https://github.com/Walter0697/DTK/pull/6))

### Added

- add TOML, YAML, and XAML command-output support to `dtk exec`
- add optional config `format` override for structured payload parsing
- add bundled Cargo.lock-style TOML, Python manifest TOML, XAML ResourceDictionary, and Kubernetes YAML sample configs to `dtk install-dummy`
- add bundled CSV inventory export sample config and parser support to `dtk install-dummy`
- add bundled INI plugin registry sample config and parser support to `dtk install-dummy`
- add bundled Terraform-style HCL variable sample config and parser support to `dtk install-dummy`
- add bundled XML RSS feed sample config and parser support to `dtk install-dummy`

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
