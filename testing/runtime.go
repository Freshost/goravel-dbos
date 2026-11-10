// Package testing provides small fakes for application unit tests. It does not
// emulate DBOS durability; PostgreSQL integration tests remain necessary for
// transaction and recovery guarantees.
package testing

import (
	"errors"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"

	packagecontracts "github.com/freshost/goravel-dbos/contracts"
)

type Runtime struct {
	ClientValue     dbossdk.Client
	ClientError     error
	RoleValue       string
	AppNameValue    string
	VersionValue    string
	ConnectionValue string
	DriverValue     string
	SchemaValue     string
	ClientCalls     int
}

func (r *Runtime) Client() (dbossdk.Client, error) {
	r.ClientCalls++
	if r.ClientError != nil {
		return nil, r.ClientError
	}
	if r.ClientValue == nil {
		return nil, errors.New("goravel-dbos testing: no ClientValue configured")
	}
	return r.ClientValue, nil
}

func (r *Runtime) Role() string               { return r.RoleValue }
func (r *Runtime) ApplicationName() string    { return r.AppNameValue }
func (r *Runtime) ApplicationVersion() string { return r.VersionValue }
func (r *Runtime) DatabaseConnection() string { return r.ConnectionValue }
func (r *Runtime) DatabaseDriver() string     { return r.DriverValue }
func (r *Runtime) DatabaseSchema() string     { return r.SchemaValue }

var _ packagecontracts.Runtime = (*Runtime)(nil)
var _ packagecontracts.ApplicationRuntime = (*Runtime)(nil)
