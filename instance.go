/*
Copyright 2026 The kcp Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kcptc

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"

	"github.com/kcp-dev/logicalcluster/v3"
	apisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	apisv1alpha2 "github.com/kcp-dev/sdk/apis/apis/v1alpha2"
	corev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	tenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"
	kcptesting "github.com/kcp-dev/sdk/testing"
)

// Instance is a running sharded kcp: cache-server, two shards and a front-proxy.
type Instance struct {
	Cache      testcontainers.Container
	FrontProxy testcontainers.Container
	ShardA     testcontainers.Container
	ShardB     testcontainers.Container

	base       *rest.Config
	shardAdmin keyPair
}

// Run starts a private kcp instance.
func Run(ctx context.Context, opts Options) (*Instance, error) { //nolint:gocritic // Options by value mirrors testcontainers' request API
	s, err := newSharded(uuid.NewString(), &opts)
	if err != nil {
		return nil, fmt.Errorf("creating instance: %w", err)
	}
	if err := s.start(ctx); err != nil {
		return nil, fmt.Errorf("starting instance: %w", err)
	}
	return &Instance{
		Cache:      s.cache,
		FrontProxy: s.frontProxy,
		ShardA:     s.shardA,
		ShardB:     s.shardB,
		base:       s.base,
		shardAdmin: s.shardAdmin,
	}, nil
}

// Shared starts a shared kcp instance.
// Within a go test run instances are shared across processes via the name.
//
// If the passed options differ then the first process to create the
// instance wins. There is no drift check.
func Shared(ctx context.Context, name string, opts Options) (*Instance, error) { //nolint:gocritic // Options by value mirrors testcontainers' request API
	s, err := newSharded(name, &opts)
	if err != nil {
		return nil, fmt.Errorf("creating instance: %w", err)
	}
	if err := s.start(ctx); err != nil {
		return nil, fmt.Errorf("starting instance: %w", err)
	}
	return &Instance{
		Cache:      s.cache,
		FrontProxy: s.frontProxy,
		ShardA:     s.shardA,
		ShardB:     s.shardB,
		base:       s.base,
		shardAdmin: s.shardAdmin,
	}, nil
}

// Config returns an admin rest.Config for the root workspace through the front-proxy.
func (in *Instance) Config() *rest.Config {
	return in.RESTConfig(ShardA)
}

// BaseConfig returns an admin rest.Config through the front-proxy that is
// not scoped to a workspace, for clients that append /clusters/<path>
// themselves or that are pointed at a URL kcp advertises, such as a
// virtual workspace URL from an APIExportEndpointSlice.
func (in *Instance) BaseConfig() *rest.Config {
	return rest.CopyConfig(in.base)
}

// RESTConfig returns an admin rest.Config for the workspace at path
// through the front-proxy.
func (in *Instance) RESTConfig(path string) *rest.Config {
	cfg := rest.CopyConfig(in.base)
	cfg.Host = in.base.Host + "/clusters/" + path
	return cfg
}

// defaultScheme returns the Kubernetes scheme extended with kcp's
// tenancy, core and apis groups.
func defaultScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(tenancyv1alpha1.AddToScheme(s))
	utilruntime.Must(corev1alpha1.AddToScheme(s))
	utilruntime.Must(apisv1alpha1.AddToScheme(s))
	utilruntime.Must(apisv1alpha2.AddToScheme(s))
	return s
}

// Client returns a controller-runtime client for the workspace at path.
// A nil opts.Scheme defaults to the Kubernetes types plus kcp's tenancy,
// core and apis groups.
func (in *Instance) Client(path string, opts client.Options) (client.Client, error) { //nolint:gocritic // mirrors client.New
	if opts.Scheme == nil {
		opts.Scheme = defaultScheme()
	}
	cl, err := client.New(in.RESTConfig(path), opts)
	if err != nil {
		return nil, fmt.Errorf("creating client for %q: %w", path, err)
	}
	return cl, nil
}

// ShardNames returns the names of the shards.
func (in *Instance) ShardNames() []string {
	// Returning the static list instead of listing the Shard objects
	// since the list _should_ be static for this setup.
	return []string{ShardA, ShardB}
}

// ShardConfig returns a system:masters rest.Config talking to shard name
// directly, bypassing the front-proxy.
func (in *Instance) ShardConfig(ctx context.Context, name string) (*rest.Config, error) {
	container, port, err := in.component(name)
	if err != nil {
		return nil, err
	}
	return in.externalConfig(ctx, container, name, port, in.shardAdmin)
}

// ExternalURL rewrites an in-network URL, e.g. a virtual workspace URL
// https://shard-b:6444/services/..., to the host-mapped endpoint.
func (in *Instance) ExternalURL(ctx context.Context, inNetwork string) (string, error) {
	u, err := url.Parse(inNetwork)
	if err != nil {
		return "", fmt.Errorf("parsing URL %q: %w", inNetwork, err)
	}
	container, port, err := in.component(u.Hostname())
	if err != nil {
		return "", err
	}
	address, err := mappedAddress(ctx, container, port)
	if err != nil {
		return "", fmt.Errorf("resolving %s address: %w", u.Hostname(), err)
	}
	u.Host = address
	return u.String(), nil
}

// CreateWorkspace creates the workspace at path, including missing parents.
// A path segment ending in "-" is used as a GenerateName prefix.
// The full path with generated names is returned.
// Options apply only to the last workspace in the path.
func (in *Instance) CreateWorkspace(ctx context.Context, path string, opts ...kcptesting.UnprivilegedWorkspaceOption) (logicalcluster.Path, error) {
	segments := strings.Split(path, ":")
	if segments[0] != ShardA {
		return logicalcluster.Path{}, fmt.Errorf("workspace path %q must start with %s", path, ShardA)
	}
	if len(segments) < 2 {
		return logicalcluster.Path{}, fmt.Errorf("workspace path %q names no workspace to create", path)
	}

	parent := logicalcluster.NewPath(segments[0])
	for i, segment := range segments[1:] {
		var segmentOpts []kcptesting.UnprivilegedWorkspaceOption
		if i == len(segments)-2 {
			segmentOpts = opts
		}
		name, err := in.createChildWorkspace(ctx, parent, segment, segmentOpts...)
		if err != nil {
			return logicalcluster.Path{}, fmt.Errorf("creating workspace %q in %s: %w", segment, parent, err)
		}
		parent = parent.Join(name)
	}
	return parent, nil
}

func (in *Instance) createChildWorkspace(ctx context.Context, parent logicalcluster.Path, name string, opts ...kcptesting.UnprivilegedWorkspaceOption) (string, error) {
	cl, err := in.Client(parent.String(), client.Options{})
	if err != nil {
		return "", err
	}

	ws := &tenancyv1alpha1.Workspace{}
	if strings.HasSuffix(name, "-") {
		ws.GenerateName = name
	} else {
		ws.Name = name
	}
	for _, opt := range opts {
		opt(ws)
	}
	if err := cl.Create(ctx, ws); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("creating workspace: %w", err)
	}

	// wait until the phase is ready
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, time.Minute, true,
		func(ctx context.Context) (bool, error) {
			current := &tenancyv1alpha1.Workspace{}
			if err := cl.Get(ctx, client.ObjectKey{Name: ws.Name}, current); err != nil {
				return false, fmt.Errorf("getting workspace: %w", err)
			}
			return current.Status.Phase == corev1alpha1.LogicalClusterPhaseReady, nil
		},
	); err != nil {
		return "", fmt.Errorf("waiting for workspace %q to become ready: %w", ws.Name, err)
	}

	// TODO(ntnn): wait until lc is accessible through front-proxy

	return ws.Name, nil
}

func (in *Instance) externalConfig(ctx context.Context, container testcontainers.Container, alias, port string, clientCert keyPair) (*rest.Config, error) {
	address, err := mappedAddress(ctx, container, port)
	if err != nil {
		return nil, fmt.Errorf("resolving %s address: %w", alias, err)
	}

	cfg := rest.CopyConfig(in.base)
	cfg.Host = "https://" + address
	cfg.CertData = clientCert.cert
	cfg.KeyData = clientCert.key
	return cfg, nil
}

func (in *Instance) component(alias string) (testcontainers.Container, string, error) {
	switch alias {
	case AliasCache:
		return in.Cache, portCache, nil
	case AliasFrontProxy:
		return in.FrontProxy, portFrontProxy, nil
	case ShardA:
		return in.ShardA, portShard, nil
	case ShardB:
		return in.ShardB, portShard, nil
	default:
		return nil, "", fmt.Errorf("no component for alias %q", alias)
	}
}

func mappedAddress(ctx context.Context, container testcontainers.Container, port string) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("resolving container host: %w", err)
	}

	mapped, err := container.MappedPort(ctx, port+"/tcp")
	if err != nil {
		return "", fmt.Errorf("resolving mapped port %s: %w", port, err)
	}

	return net.JoinHostPort(host, mapped.Port()), nil
}
