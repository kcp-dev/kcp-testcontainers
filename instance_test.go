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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"k8s.io/client-go/rest"

	tenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"
)

func TestInstance_RESTConfig(t *testing.T) {
	t.Parallel()

	in := &Instance{
		base: &rest.Config{
			Host: "https://localhost:12345",
			TLSClientConfig: rest.TLSClientConfig{
				CAData:   []byte("ca"),
				CertData: []byte("cert"),
				KeyData:  []byte("key"),
			},
		},
	}

	cfg := in.RESTConfig("root:my:workspace")
	assert.Equal(t, "https://localhost:12345/clusters/root:my:workspace", cfg.Host)
	assert.Equal(t, in.base.TLSClientConfig, cfg.TLSClientConfig, "client credentials must be carried over")
	cfg.Host = "https://changed.example"
	assert.Equal(t, "https://localhost:12345", in.base.Host, "base must not be mutated")

	assert.Equal(t, "https://localhost:12345/clusters/root", in.Config().Host)

	base := in.BaseConfig()
	assert.Equal(t, "https://localhost:12345", base.Host, "BaseConfig must not be scoped")
	assert.Equal(t, in.base.TLSClientConfig, base.TLSClientConfig, "client credentials must be carried over")
	base.Host = "https://changed.example"
	assert.Equal(t, "https://localhost:12345", in.base.Host, "base must not be mutated")
}

func TestInstance_Client(t *testing.T) {
	t.Parallel()

	in := &Instance{
		base: &rest.Config{
			Host: "https://localhost:12345",
		},
	}

	cl, err := in.Client("root", client.Options{})
	require.NoError(t, err)

	_, _, err = cl.Scheme().ObjectKinds(&tenancyv1alpha1.Workspace{})
	require.NoError(t, err, "default scheme must know kcp types")
}

func TestInstance_ShardNames(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{ShardA, ShardB}, (&Instance{}).ShardNames())
}

func TestInstance_ShardConfig(t *testing.T) {
	t.Parallel()

	_, err := (&Instance{}).ShardConfig(t.Context(), "unknown")
	require.Error(t, err, "unknown shards must fail before contacting Docker")
}

func TestInstance_ExternalURL(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"malformed URL":     "https://%invalid",
		"unknown component": "https://unknown:6444/services",
		"missing host":      "/services",
	}
	for title, input := range cases {
		t.Run(title, func(t *testing.T) {
			t.Parallel()

			_, err := (&Instance{}).ExternalURL(t.Context(), input)
			require.Error(t, err, "invalid URLs must fail before contacting Docker")
		})
	}
}

func TestInstance_component(t *testing.T) {
	t.Parallel()

	in := &Instance{
		Cache:      &testcontainers.DockerContainer{ID: AliasCache},
		FrontProxy: &testcontainers.DockerContainer{ID: AliasFrontProxy},
		ShardA:     &testcontainers.DockerContainer{ID: ShardA},
		ShardB:     &testcontainers.DockerContainer{ID: ShardB},
	}
	cases := map[string]struct {
		container testcontainers.Container
		port      string
	}{
		AliasCache:      {in.Cache, portCache},
		AliasFrontProxy: {in.FrontProxy, portFrontProxy},
		ShardA:          {in.ShardA, portShard},
		ShardB:          {in.ShardB, portShard},
	}
	for alias, cas := range cases {
		t.Run(alias, func(t *testing.T) {
			t.Parallel()

			container, port, err := in.component(alias)
			require.NoError(t, err)
			assert.Same(t, cas.container, container)
			assert.Equal(t, cas.port, port)
		})
	}
}

func TestInstance_CreateWorkspace_invalidPath(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"not rooted":   "other:ws",
		"root only":    ShardA,
		"empty":        "",
		"wrong casing": "Root:ws",
	}

	for title, path := range cases {
		t.Run(title, func(t *testing.T) {
			t.Parallel()

			in := &Instance{}
			_, err := in.CreateWorkspace(t.Context(), path)
			require.Error(t, err, "path %q must be rejected before any API call", path)
		})
	}
}
