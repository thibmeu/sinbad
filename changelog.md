# Changelog

Notable changes to Sinbad, following [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Fixed

- The Leader splits aggregation jobs that would exceed the Helper's 1 MiB request limit, instead of losing their reports.
- A failed HPKE configuration fetch no longer stops later tracking for that task.
- An invalid measurement no longer drops the valid ones batched with it.
- Reports retried after a key refresh keep their original time.
- Aggregator storage is bound to its task ID and role, not only the task configuration.

## [0.1.0] - 2026-09-30

First release, built on @thibmeu/dap 0.1.0 (DAP draft 19).

### Added

- `Sinbad` and `createSiteAnalytics()` for browser page views, events, and bounded values, driven by a site manifest.
- Batched, keepalive delivery with one HPKE configuration fetch per aggregator.
- `sinbad/fetch` for uploads, HPKE configuration retrieval, and collection requests.
- `sinbad/collector` to poll collection jobs and resume pending ones.
- `sinbad/aggregator`, a Leader and Helper over synchronous SQLite, with example servers.

[0.1.0]: https://github.com/thibmeu/sinbad/releases/tag/v0.1.0
