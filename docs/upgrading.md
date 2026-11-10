# Upgrading to goravel-dbos 0.2

This release updates DBOS Go from 1.0.0 to
[1.8.0](https://github.com/dbos-inc/dbos-transact-golang/releases/tag/v1.8.0),
the latest stable release checked on 2026-11-10. Go 1.25 and Goravel 1.18 remain
the supported baseline. PostgreSQL remains the only production-supported backend;
SQLite still requires explicit experimental opt-in.

## Compatibility assessment

| Upstream change since 1.0 | Integration response |
| --- | --- |
| Shutdown no longer durably cancels pending work; recovery uses queues; child deadlines and transient transaction errors have fixes | Upgrade rather than carry local recovery patches |
| Application ownership for workflows, queues and schedules | Scope producer Clients and package ingress enqueue to the configured application; pass ownership to PostgreSQL transactional enqueue |
| Workflow payloads moved into separate SDK tables | Continue using the public `enqueue_workflow` function and SDK APIs; let SDK migrations own the storage layout |
| Standalone Clients verify the schema without migrating it | Retain producer startup `Prepare` through a migration-only context; keep `dbos:migrate` as the explicit deployment operation |
| Admin HTTP server removed | Retain deprecated public Config fields for source compatibility; reject enabling the server and remove it from newly published config |
| Conductor moved into the optional Enterprise module | Applications using Conductor add the matching extension explicitly; ordinary PostgreSQL workflows require no commercial extension |
| Durable child enqueue lineage, messaging, parallel steps and workflow patching | Use native SDK primitives directly from application workflow contexts; no graph DSL or product policy is added |

The public `contracts.Runtime` interface and existing workflow declarations,
registration and enqueue signatures remain unchanged. `Runtime.ApplicationName`
and the additive `contracts.ApplicationRuntime` interface expose ownership.

## Deployment steps

1. Back up the system database. Stop old workers and pause producers before
   applying the SDK schema migrations. Do not run DBOS 1.0 processes against the
   migrated database. Plan rollback as a database restore together with the
   previous application binary, rather than a dependency downgrade alone.
2. Set a stable `DBOS_APP_NAME` containing 3–256 lowercase letters, numbers,
   dashes or underscores, identically in producers and workers. New upstream
   Conductor validation rejects names such as `FreshAgents` or `fresh.agents`;
   self-hosted SDK contexts warn and continue, preserving existing names. Existing
   1.0 records have no application owner and remain readable/recoverable;
   continue using the existing database/schema for that application. Ownership
   is routing metadata, not tenant authorization or isolation of legacy rows.
3. Keep `DBOS_APP_VERSION` and executor identities compatible with pending work.
   For an SDK-only rollout, retain the existing application version when
   workflow code is compatible. Do not silently reroute old execution histories
   to a new, incompatible graph implementation.
4. Disable `dbos.admin_server.enabled` / `DBOS_ADMIN_SERVER_ENABLED` if previously
   set. The SDK no longer serves `/dbos-healthz`. Replace that probe with the
   consuming application's worker lifecycle/readiness integration and monitor
   queue progress separately. No replacement HTTP server is added by this package.
5. Run `./artisan dbos:migrate` using the new binary before accepting ingress.
   Run `./artisan dbos:doctor`, then start the compatible workers and producers.
   A direct `Runtime.Client()` now requires a current schema; it does not migrate.
   Package runners still call `Prepare()` at startup as defense in depth.
6. If you supply your own `contracts.Runtime` implementation, add
   `ApplicationName() string` before using PostgreSQL `EnqueueInTransaction`.
   A missing identity fails before executing SQL. The package's concrete Runtime
   already provides it. Test fakes can set `testing.Runtime.AppNameValue`.
7. If using DBOS Conductor, link `go.dbos.dev/dbos-enterprise-go` at the same
   minor version as the SDK (1.8), with a blank import in your application.
   The SDK now requires that optional module to connect. Without Conductor,
   normal workflow execution and local restart recovery remain available in
   the open-source SDK without this dependency.

Package enqueue helpers pin both the application version and (when available)
application name. Use the raw SDK Client explicitly for intentional routing to
another application. Workflow registration, application migrations and graph
definitions stay in the consuming application.

## Release verification

CI provisions isolated PostgreSQL 17 and runs formatting, module tidy, vet,
unit/integration tests, the required `make test-postgres` gate and race tests.
It builds `testdata/upgrade` with DBOS 1.0 independently from the library and
provides its binary through `GORAVEL_DBOS_V1_FIXTURE`. The upgrade test uses that
binary to create an old schema and crash with pending SDK-enqueued and
SQL-enqueued workflows. After migration, 1.8 workers must resume them without
repeating completed steps.

Additional PostgreSQL coverage exercises transaction commit/rollback and payload
execution, application ownership, concurrent children, parent lineage, durable
human replies while workers are down and restart recovery. Experimental SQLite
coverage remains separate from the production support guarantee. All builds,
tests and automated checks run in CI; no local Mac execution is required.
