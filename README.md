# goravel-dbos

[![Release](https://img.shields.io/github/v/release/Freshost/goravel-dbos?sort=semver)](https://github.com/Freshost/goravel-dbos/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/freshost/goravel-dbos.svg)](https://pkg.go.dev/github.com/freshost/goravel-dbos)
[![Go Report Card](https://goreportcard.com/badge/github.com/freshost/goravel-dbos)](https://goreportcard.com/report/github.com/freshost/goravel-dbos)
[![Built for Goravel](https://img.shields.io/badge/built%20for-Goravel-00B7EB)](https://www.goravel.dev)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)
[![Changelog](https://img.shields.io/badge/changelog-keep--a--changelog-orange)](CHANGELOG.md)

`goravel-dbos` is the native Goravel 1.18 integration layer for DBOS Transact
Go 1.x. It standardizes configuration, producer/worker lifecycle, typed workflow
contracts, PostgreSQL/SQLite transaction-bound enqueue and graceful shutdown. Workflow
implementations and domain recovery remain in the consuming app.

DBOS always uses Goravel's native `database.default` connection. PostgreSQL is
the fully supported production database. File-backed SQLite is available only
as an explicit experimental opt-in. A separate DBOS database and separate DBOS
credentials are intentionally unsupported.

## Support status

The PostgreSQL integration is release-candidate quality and is the only
production support tier. Every release must pass the unit, race and vet suites
plus the real PostgreSQL commit/rollback and producer/worker lifecycle tests in
CI before it is tagged. SQLite remains experimental regardless of test status.

Start consumer adoption with one canary application before rolling the package
out across all FreshStack services.

## Requirements

- Go 1.25 or newer
- Goravel 1.18
- DBOS Transact Go 1.8
- PostgreSQL
- Optionally, file-backed SQLite in experimental mode

### Experimental SQLite support

SQLite is disabled by default. Enable it explicitly only for local development,
tests or evaluated low-risk deployments:

```dotenv
DBOS_SQLITE_EXPERIMENTAL=true
```

The current Goravel SQLite module uses `ncruces/go-sqlite3`, while DBOS Go uses
`modernc.org/sqlite`. Both access the same file through separate connection
pools. The transactional dispatch behavior is integration-tested, but this
dual-driver combination is not covered by the package's production support
guarantee. Do not place the SQLite database on NFS or another network
filesystem. PostgreSQL remains the required choice for production FreshStack
applications.

## Installation

```shell
go get github.com/freshost/goravel-dbos
./artisan package:install github.com/freshost/goravel-dbos
```

The installer registers `goraveldbos.ServiceProvider` and publishes
`config/dbos.go`.

It does not modify consuming application code, move existing workflow
implementations or create application database migrations. Each application
adopts the package and migrates its own integration independently.

### Prepare the DBOS system schema

Run the package-owned migration command as an explicit release step after the
application configuration is available and before producers or workers accept
traffic:

```shell
./artisan dbos:migrate
```

The command initializes or upgrades only the internal schema owned by the DBOS
SDK using a migration-only context that does not start a notification listener
or workflow executor. It never runs or publishes migrations owned by the
consuming application. Active package runners also call the
idempotent `Runtime.Prepare()` on startup for fail-fast connectivity, but
Goravel runners start concurrently; startup preparation is defense in depth,
not a replacement for the explicit deployment step when using
`EnqueueInTransaction`.

## Process roles

| Role | Behavior |
| --- | --- |
| `producer` | Initializes a DBOS Client at process start; on SQLite also runs the internal dispatch relay. Executes no workflows. |
| `worker` | Registers and launches DBOS; disables standard Goravel server/scheduler/queue runners. |
| `embedded` | Launches DBOS alongside the application's other runners. |
| `disabled` | Does not connect to DBOS. |

Use `producer` for web and scheduler processes and a separate `worker` process
in production. `embedded` exists for compatibility and small deployments.

## Declare, register and enqueue a workflow

```go
var Reconcile = goraveldbos.Define[ReconcileInput, ReconcileOutput](
    "freshproxy.reconcile.v1",
    "reconcile",
)

func registerDBOS(registry *goraveldbos.Registry) error {
    if _, err := registry.Queue(
        Reconcile.Queue(),
        dbos.WithGlobalConcurrency(1),
    ); err != nil {
        return err
    }
    return goraveldbos.RegisterWorkflow(registry, Reconcile, reconcileWorkflow)
}

handle, err := goraveldbos.Enqueue(runtime, Reconcile, input,
    dbos.WithEnqueueWorkflowID(stableID))
```

Add the registrar from an application service provider during `Boot`:

```go
err := goraveldbos.Register(app, goraveldbos.RegistrarFunc(registerDBOS))
```

Only executor roles invoke registrars. A producer enqueues by the stable name in
the definition and therefore does not need the workflow implementation linked
into its runtime registry.

## Native atomic enqueue with a business transaction

The package derives the DBOS connection from Goravel's native
`database.default` configuration and enables:

```go
workflowID, err := goraveldbos.EnqueueInTransaction(
    runtime,
    tx,
    InvoicePaid,
    input,
    goraveldbos.WithTransactionWorkflowID(input.InvocationID),
)
```

The application keeps one API on both databases:

- PostgreSQL calls DBOS's public `enqueue_workflow` function in the active
  business transaction, so the DBOS row commits or rolls back directly.
- Experimental SQLite writes a package-owned internal dispatch journal in the
  active business transaction. The package relay delivers it to DBOS after commit
  using the same stable workflow ID, leases and durable retry. Rollback removes
  both the business write and dispatch intent.

No application outbox API or application migration is required. The package
creates and versions its private SQLite tables automatically. `:memory:` SQLite
is rejected because Goravel and DBOS cannot safely share it across their
separate driver pools. `WithTransactionDeadline` is unavailable on SQLite; use
`WithTransactionTimeout` for portable behavior.

Use ordinary `Enqueue` when no application database write must be committed with
the workflow. Whenever a business write and workflow creation form one
invariant, call `EnqueueInTransaction` inside that same short transaction.

## Configuration and operations

Production requires a stable `DBOS_APP_NAME` and `DBOS_APP_VERSION`. Do not rely
on DBOS's binary-derived version when producers and workers are separate
binaries. `DBOS__APPVERSION` and `DBOS__VMID`, when present, must agree with the
package configuration.

The selected `database.default` connection must use PostgreSQL, or file-backed
SQLite with `dbos.sqlite.experimental=true`. For PostgreSQL, both normal Goravel
connection fields and native `dsn` are supported, including a Unix socket path
such as `/run/postgresql` in the native `host` field. For experimental SQLite,
the package reuses `dsn` or `database`. A disabled DBOS runtime does not require
a database connection.

DBOS needs its own PostgreSQL pool even though it reuses the native Goravel DSN
and database. Tune that connection budget directly in `config/dbos.go`; these
defaults match DBOS Go 1.8:

```go
"pool": map[string]any{
    "max_conns":                   20,
    "min_conns":                   0,
    "max_conn_lifetime_seconds":  3600,
    "max_conn_idle_time_seconds": 300,
    "connect_timeout_seconds":    10,
},
```

These settings do not permit separate DBOS credentials or a separate database.

Check configuration, schema migration and connectivity without exposing the
database URL:

```shell
./artisan dbos:doctor
```

DBOS Clients verify the schema without migrating it. Use `dbos:migrate` as the
explicit release operation before `dbos:doctor` or direct `Runtime.Client()`
calls. Producer runners retain automatic startup preparation through `Prepare`.

DBOS Go 1.8 no longer provides the admin HTTP server or `/dbos-healthz`.
Integrate worker readiness with the consuming application's lifecycle and
monitor queue progress separately. Enabling the removed server fails with a
migration instruction.

The DBOS SDK owns its internal system storage. On SQLite, this package also owns
and automatically versions `goravel_dbos_package_migrations` and
`goravel_dbos_outbox`; neither is an application table. The package does not
publish application Goravel migrations or HTTP management routes.
Applications may build their own authorized jobs UI over the Client returned by
`Runtime.Client()`.

## Testing

Use `github.com/freshost/goravel-dbos/testing.Runtime` for narrow unit-test
injection. Verify production PostgreSQL behavior against PostgreSQL. The package
also tests experimental SQLite against a real file-backed database, including
commit, rollback and relay idempotency, but those tests do not promote SQLite
to the production support tier.

The package's PostgreSQL commit/rollback test runs when
`GORAVEL_DBOS_TEST_DATABASE_URL` is set; otherwise it is skipped by the normal
unit-test suite.

All builds, tests and automated checks run in CI. GitHub Actions provisions
isolated PostgreSQL 17 and runs tidy, vet, the full test and race suites, and
the explicit `make test-postgres` release gate. It also builds a DBOS 1.0 fixture
to verify migration and recovery from the previous SDK's persisted state.

## Agent teams and upgrades

Use native DBOS steps, queued child workflows and durable messages for
sequential/parallel agent execution and human input. Graph definitions and
product behavior belong to the consuming platform. See
[application-owned workflows](docs/workflows.md) for composition and recovery
boundaries, and [upgrading to 0.2](docs/upgrading.md) before updating an existing
application.
