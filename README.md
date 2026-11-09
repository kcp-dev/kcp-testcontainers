# kcp-testcontainers

[testcontainers-go](https://golang.testcontainers.org/) module running a sharded [kcp](https://kcp.io) instance for tests.

A kcp-testcontainers instance is always deployed under the constraints
the kcp project defines as the minimum viable architecture:

- At least one front-proxy
- At least two shards
- One cache-server

With the front-proxy and the shards' virtual workspaces being accessible to clients.

This testcontainer module is ideal to write integration or e2e tests for operators targeting kcp directly or a system utilizing kcp.

## Usage

Import this module:

```go
import kcptc "github.com/kcp-dev/kcp-testcontainers"
```

<!--
```go ci
package kcptc_test

import  (
	"context"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kcptc "github.com/kcp-dev/kcp-testcontainers"
)

var _ = func(ctx context.Context) {
```
-->

Create an instance and create workspaces:

```go ci
instance, err := kcptc.Run(ctx, kcptc.Options{})

// create workspaces (parents included)
path, err := instance.CreateWorkspace(ctx, "root:my:workspace")
// create workspace with generated name (parents included)
path, err = instance.CreateWorkspace(ctx, "root:test-")

// rest.Config for a workspace
cfg := instance.RESTConfig("root:my:workspace")
// controller-runtime client for a workspace
cl, err := instance.Client("root:my:workspace", client.Options{})
```

<!--
```go ci
    // prevent `go vet` from erroring on unused variabled
    _ = path
    _ = err
    _ = cfg
    _ = cl
```
-->

## Sharing across processes

When running go tests each tested package is run as its own process.

`kcptc.Shared` implements sharing instances across processes by giving
an instance a name and using testcontainers `SessionID`:

```go ci
// a shared instance that is reused across processes
shared, err := kcptc.Shared(ctx, "shared-instance", kcptc.Options{})
sharedWS, err := shared.CreateWorkspace(ctx, "root:my:workspace")
sharedCL, err := shared.Client("root:my:workspace", client.Options{})
```

<!--
```go ci
_ = shared
_ = sharedWS
_ = sharedCL
}
```
-->

To prevent drift for the configurations a good option is to have
a `framework` package with functions that call `Shared`:

```go ci
func Kcp(ctx context.Context) (*kcptc.Instance, error) {
	return kcptc.Shared(ctx, "default", kcptc.Options{})
}

func KcpWithMigrations(ctx context.Context) (*kcptc.Instance, error) {
	return kcptc.Shared(ctx, "second", kcptc.Options{
		FeatureGates: []string{"LogicalClusterMigration"},
	})
}
```
