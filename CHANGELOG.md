# Changelog

All notable changes to `goravel-dbos` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the package is pre-1.0, minor versions may contain breaking changes.

## [Unreleased]

## [0.2.0] - 2026-11-10

### Changed

- Upgrade DBOS Go from 1.0.0 to 1.8.0, retaining Go 1.25 and Goravel 1.18.
- Scope producer Clients and package ingress enqueue to the application name;
  include ownership in PostgreSQL's public transactional enqueue function.
- Preserve producer startup schema preparation through a migration-only context;
  direct Clients now verify the schema without migrating it.
- Validate upstream's application-name format. Reject enabling the removed DBOS
  admin server; retain deprecated Config fields for source compatibility.

### Added

- `Runtime.ApplicationName`, additive `contracts.ApplicationRuntime` and
  `testing.Runtime.AppNameValue`. Custom runtimes must expose an application
  name for PostgreSQL transactional enqueue; missing identity fails closed.
- PostgreSQL verification of DBOS 1.0 schema/history upgrades, concurrent child
  workflows, lineage, durable human input and restart recovery.
- Application workflow composition and [upgrade instructions](docs/upgrading.md),
  including the replacement of `/dbos-healthz` probes and deployment ordering.

## [0.1.2] - 2026-08-31

### Fixed

- `dbos:migrate` now uses a migration-only DBOS context, preventing a benign
  notification-listener cancellation from being logged as an error during
  successful command shutdown.

## [0.1.1] - 2026-08-31

### Added

- Idempotent `Runtime.Prepare` and explicit `dbos:migrate` for DBOS SDK-owned
  system-schema preparation before producers accept transactional traffic.
- Native DBOS PostgreSQL pool tuning while retaining Goravel's
  `database.default` DSN and credentials.
- Real Goravel `facades.Orm()` PostgreSQL commit/rollback integration coverage.

### Fixed

- Producer startup now initializes the DBOS Client instead of leaving the first
  transaction-bound enqueue exposed to a missing-schema race.
- Native PostgreSQL connection derivation now supports Unix socket directories.

## [0.1.0] - 2026-08-31

### Added

- Native Goravel service provider, facade, runner and package installer.
- Typed workflow declarations, registration and enqueue helpers.
- PostgreSQL transaction-bound enqueue using DBOS system-database functions.
- Experimental file-backed SQLite transaction journal and relay.
- Producer, worker, embedded and disabled process roles.
- Configuration and connectivity diagnostics through `dbos:doctor`.
