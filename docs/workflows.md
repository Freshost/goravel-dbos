# Application-owned workflows

DBOS Go 1.8 supplies the execution primitives needed for agent teams.
`goravel-dbos` owns Goravel configuration, registration, producer/worker lifecycle
and atomic workflow creation. The consuming platform owns its graph definition,
authorization, agent protocols, approval policy and application migrations.

## Choosing primitives

| Need | DBOS API |
| --- | --- |
| Sequential durable operations | `RunAsStep`, or start a child and call its `GetResult` before starting the next |
| Parallel agent workflows | Start all children with `Enqueue` using the workflow context, then await their handles |
| Parallel steps / first completed step | `Go` / `Select` |
| Agent callback or human reply | `Send` / `Recv` with a topic |
| Readable workflow progress | `SetEvent` / `GetEvent` |
| Durable delay | `Sleep` |
| Resume after a restart | Register the same workflow names and launch a worker with compatible code and the same application version |

Use `goraveldbos.Enqueue(runtime, definition, input)` at the application ingress.
Inside a workflow, pass the **current workflow context** to SDK operations. The
runtime's root Client does not carry the parent's checkpoint state or lineage.
For example, after registering `Agent` and its queue with the package registry:

```go
func team(ctx dbos.Context, tasks []AgentInput) ([]AgentResult, error) {
    handles := make([]dbos.WorkflowHandle[AgentResult], 0, len(tasks))
    for _, task := range tasks {
        handle, err := dbos.Enqueue[AgentResult](ctx, Agent.Queue(), Agent.Name(), task)
        if err != nil {
            return nil, err
        }
        handles = append(handles, handle)
    }

    results := make([]AgentResult, 0, len(handles))
    for _, handle := range handles {
        result, err := handle.GetResult()
        if err != nil {
            return nil, err
        }
        results = append(results, result)
    }
    return results, nil
}
```

This dispatches every child before waiting; agents execute concurrently and
results remain in input order. For a sequential dependency, await one child's
result before starting the next. Queued child workflows require no additional
Go goroutines. Give waiting orchestrators and agents separate queues and enough
worker capacity: a parent waiting for children must not occupy the only slot
those children need.

For an approval, call `dbos.Recv[Approval](ctx, "approval", timeout)` inside the
workflow. An authorized HTTP handler or callback obtains `runtime.Client()` and
calls `dbos.Send(client, workflowID, approval, "approval",
dbos.WithIdempotencyKey(replyID))`. The message persists even while workers are
down. Persist and reuse the same reply ID when retrying delivery. Choose an
explicit timeout and handle its error according to the platform's policy.

## Recovery boundaries

- Put LLM calls, RPCs, time/random reads and other external effects inside steps.
  Completed steps replay their saved results. An interrupted step can run again;
  use external idempotency keys or reconciliation for effects that may already
  have happened. DBOS cannot make a database write atomic with an external RPC.
- Keep workflow control flow deterministic. Use ordered slices or sorted map
  keys when creating steps or children. Use DBOS `Go` and `Select` for durable
  concurrency; native goroutines and `select` belong inside step bodies.
- Automatic local recovery targets the configured executor ID and application
  version. Preserve an executor's identity across restarts. For recovery across
  replaced executor IDs, operate DBOS Conductor or explicitly resume stranded
  workflows with `ResumeWorkflow` after confirming the old executor has stopped.
  Do not give live workers the same ID.
- The package pins ingress enqueue to `DBOS_APP_VERSION`. Keep compatible
  workers for versions with pending work. A new version does not automatically
  take over old work. Use DBOS patching for compatible control-flow changes and
  deliberate workflow versioning for incompatible inputs or execution histories.
  DBOS 1.8 auto-upgrade is an optional Enterprise feature and is not enabled here.
- `EnqueueInTransaction` is for short business transactions at ingress. It
  commits workflow creation with the Goravel write; do not wait for agents or
  people while holding that transaction. SDK `WithEnqueueTransaction` accepts
  raw SQL/pgx transactions, not a Goravel `orm.Query`; the package adapter keeps
  using DBOS's public PostgreSQL `enqueue_workflow` function.

See the upstream [workflow tutorial](https://docs.dbos.dev/golang/tutorials/workflow-tutorial)
and [queue tutorial](https://docs.dbos.dev/golang/tutorials/queue-tutorial).
