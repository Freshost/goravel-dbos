# Changelog

All notable changes to `goravel-dbos` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the package is pre-1.0, minor versions may contain breaking changes.

## [Unreleased]

## [0.1.0] - 2026-08-31

### Added

- Native Goravel service provider, facade, runner and package installer.
- Typed workflow declarations, registration and enqueue helpers.
- PostgreSQL transaction-bound enqueue using DBOS system-database functions.
- Experimental file-backed SQLite transaction journal and relay.
- Producer, worker, embedded and disabled process roles.
- Configuration and connectivity diagnostics through `dbos:doctor`.
