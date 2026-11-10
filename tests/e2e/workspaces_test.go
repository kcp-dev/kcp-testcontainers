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

package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"k8s.io/apimachinery/pkg/util/wait"

	kcpcrypto "github.com/kcp-dev/apimachinery/v2/pkg/util/crypto"
	corev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	tenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"
	kcptesting "github.com/kcp-dev/sdk/testing"

	kcptc "github.com/kcp-dev/kcp-testcontainers"
	"github.com/kcp-dev/kcp-testcontainers/tests/e2e/framework"
)

func TestWorkspacePinnedToShard(t *testing.T) {
	t.Parallel()

	instance, err := framework.Default(t.Context())
	require.NoError(t, err)

	t.Logf("Create a workspace pinned to shard %q", kcptc.ShardB)
	path, err := instance.CreateWorkspace(t.Context(), "root:pinned", kcptc.With(kcptesting.WithShard(kcptc.ShardB)))
	require.NoError(t, err)
	assert.Equal(t, "root:pinned", path.String())

	root, err := instance.Client(kcptc.ShardA, client.Options{})
	require.NoError(t, err)

	t.Logf("Check workspace is scheduled on shard %q", kcptc.ShardB)
	// the expected annotation and shard name
	shardAnnotation, wantShard := corev1alpha1.LogicalClusterShardAnnotationKey, kcptc.ShardB
	if framework.Before(0, 32) {
		// pre 0.32 the shard was documented on a workspace, but under
		// a different annotation and the shard name was base35'd and
		// hashed.
		shardAnnotation, wantShard = "internal.tenancy.kcp.io/shard", kcpcrypto.Base36Sha224.String(kcptc.ShardB)[:8]
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ws := &tenancyv1alpha1.Workspace{}
		require.NoError(c, root.Get(t.Context(), client.ObjectKey{Name: "pinned"}, ws))
		assert.Equal(c,
			wantShard,
			ws.Annotations[shardAnnotation],
			"workspace must be scheduled to %s", kcptc.ShardB,
		)
	}, wait.ForeverTestTimeout, 100*time.Millisecond)

	t.Log("Access pinned workspace")
	pinned, err := instance.Client(path.String(), client.Options{})
	require.NoError(t, err)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		require.NoError(c, pinned.List(t.Context(), &tenancyv1alpha1.WorkspaceList{}))
	}, wait.ForeverTestTimeout, 100*time.Millisecond)
}

func TestWorkspaceNestedParents(t *testing.T) {
	t.Parallel()

	instance, err := framework.Default(t.Context())
	require.NoError(t, err)

	t.Log("Create a workspace with parents")
	_, err = instance.CreateWorkspace(t.Context(), "root:my-org:team")
	require.NoError(t, err)

	t.Log("Creating the same path again must not error")
	_, err = instance.CreateWorkspace(t.Context(), "root:my-org:team")
	require.NoError(t, err, "creating an existing path must succeed")

	org, err := instance.Client("root:my-org", client.Options{})
	require.NoError(t, err)

	t.Log("Verify the workspace exists")
	workspaces := &tenancyv1alpha1.WorkspaceList{}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		require.NoError(c, org.List(t.Context(), workspaces))
		assert.Len(c, workspaces.Items, 1)
	}, wait.ForeverTestTimeout, 100*time.Millisecond)
	assert.Equal(t, "team", workspaces.Items[0].Name)
}

func TestWorkspaceGenerateName(t *testing.T) {
	t.Parallel()

	instance, err := framework.Default(t.Context())
	require.NoError(t, err)

	t.Log("Create a workspace with a generate name")
	path, err := instance.CreateWorkspace(t.Context(), "root:with:parents:gen-")
	require.NoError(t, err)
	assert.Regexp(t, `^root:with:parents:gen-.+`, path.String())

	t.Logf("Access workspace with generate name %q", path)
	cl, err := instance.Client(path.String(), client.Options{})
	require.NoError(t, err)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		require.NoError(c, cl.List(t.Context(), &tenancyv1alpha1.WorkspaceList{}))
	}, wait.ForeverTestTimeout, 100*time.Millisecond, "generated workspace must be usable by returned path")
}

func TestWorkspaceWaitForReachable(t *testing.T) {
	t.Parallel()

	instance, err := framework.Default(t.Context())
	require.NoError(t, err)

	t.Log("Create a workspace with parents and wait for the front-proxy to serve them")
	path, err := instance.CreateWorkspace(t.Context(), "root:reachable:child-", kcptc.WaitForReachable())
	require.NoError(t, err)

	t.Logf("Access %q and its parent without retrying", path)
	for _, p := range []string{path.String(), "root:reachable"} {
		cl, err := instance.Client(p, client.Options{})
		require.NoError(t, err)
		require.NoError(t, cl.List(t.Context(), &tenancyv1alpha1.WorkspaceList{}), "workspace %s must be served through the front-proxy on return", p)
	}
}
