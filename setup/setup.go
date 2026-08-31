package main

import (
	"os"

	"github.com/goravel/framework/packages"
	"github.com/goravel/framework/packages/modify"
	"github.com/goravel/framework/support/file"
	"github.com/goravel/framework/support/path"
)

func main() {
	setup := packages.Setup(os.Args)
	module := setup.Paths().Module().String()
	config, err := file.GetPackageContent(module, "setup/config/dbos.go")
	if err != nil {
		panic(err)
	}
	moduleImport := setup.Paths().Module().Import()
	provider := "&goraveldbos.ServiceProvider{}"
	setup.Install(
		modify.RegisterProvider(moduleImport, provider),
		modify.File(path.Config("dbos.go")).Overwrite(config),
	).Uninstall(
		modify.File(path.Config("dbos.go")).Remove(),
		modify.UnregisterProvider(moduleImport, provider),
	).Execute()
}
