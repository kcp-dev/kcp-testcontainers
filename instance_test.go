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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"k8s.io/client-go/rest"

	tenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"
)

func TestInstance_RESTConfig(t *testing.T) {
	t.Parallel()

	in := &Instance{
		sharded: &sharded{
			base: &rest.Config{
				Host: "https://localhost:12345",
				TLSClientConfig: rest.TLSClientConfig{
					ServerName: AliasFrontProxy,
				},
			},
		},
	}

	cfg := in.RESTConfig("root:my:workspace")
	assert.Equal(t, "https://localhost:12345/clusters/root:my:workspace", cfg.Host)
	assert.Equal(t, AliasFrontProxy, cfg.ServerName, "TLS settings must be carried over")
	assert.Equal(t, "https://localhost:12345", in.sharded.base.Host, "base must not be mutated")

	assert.Equal(t, "https://localhost:12345/clusters/root", in.Config().Host)
}

func TestInstance_Client(t *testing.T) {
	t.Parallel()

	in := &Instance{
		sharded: &sharded{
			base: &rest.Config{
				Host: "https://localhost:12345",
			},
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

			in := &Instance{
				sharded: &sharded{},
			}
			_, err := in.CreateWorkspace(t.Context(), path)
			require.Error(t, err, "path %q must be rejected before any API call", path)
		})
	}
}
