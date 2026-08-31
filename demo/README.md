# Demo integration

Register the package provider before the application's workflow provider:

```go
return []foundation.ServiceProvider{
    // Goravel framework and database providers...
    &goraveldbos.ServiceProvider{},
    &providers.DBOSWorkflowsProvider{},
}
```

The application provider contributes its own registrar:

```go
type DBOSWorkflowsProvider struct{}

func (p *DBOSWorkflowsProvider) Register(foundation.Application) {}

func (p *DBOSWorkflowsProvider) Boot(app foundation.Application) {
    if err := goraveldbos.Register(app, goraveldbos.RegistrarFunc(registerWorkflows)); err != nil {
        panic(err)
    }
}

func registerWorkflows(registry *goraveldbos.Registry) error {
    if _, err := registry.Queue("example", dbos.WithWorkerConcurrency(4)); err != nil {
        return err
    }
    return goraveldbos.RegisterWorkflow(registry, ExampleWorkflow, runExample)
}
```

Run the web process with `DBOS_ROLE=producer` and a separate process built from
the same application with `DBOS_ROLE=worker`. The worker role disables Goravel's
HTTP, gRPC, scheduler and native queue runners by default.
