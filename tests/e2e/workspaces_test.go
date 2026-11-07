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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	tenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"
	kcptesting "github.com/kcp-dev/sdk/testing"

	kcptc "github.com/kcp-dev/kcp-testcontainers"
)

func TestWorkspacePinnedToShard(t *testing.T) {
	t.Parallel()

	instance, err := kcptc.Run(t.Context(), kcptc.Options{Image: image})
	require.NoError(t, err)

	t.Logf("Create a workspace pinned to shard %q", kcptc.ShardB)
	path, err := instance.CreateWorkspace(t.Context(), "root:pinned", kcptesting.WithShard(kcptc.ShardB))
	require.NoError(t, err)
	assert.Equal(t, "root:pinned", path.String())

	root, err := instance.Client(kcptc.ShardA, client.Options{})
	require.NoError(t, err)

	t.Logf("Check workspace is scheduled on shard %q", kcptc.ShardB)
	ws := &tenancyv1alpha1.Workspace{}
	require.NoError(t, root.Get(t.Context(), client.ObjectKey{Name: "pinned"}, ws))
	assert.Equal(t,
		kcptc.ShardB,
		ws.Annotations[corev1alpha1.LogicalClusterShardAnnotationKey],
		"workspace must be scheduled to %s", kcptc.ShardB,
	)

	t.Log("Access pinned workspace")
	pinned, err := instance.Client(path.String(), client.Options{})
	require.NoError(t, err)
	require.NoError(t, pinned.List(t.Context(), &tenancyv1alpha1.WorkspaceList{}))
}

func TestWorkspaceNestedParents(t *testing.T) {
	t.Parallel()

	instance, err := kcptc.Run(t.Context(), kcptc.Options{Image: image})
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
	require.NoError(t, org.List(t.Context(), workspaces))
	require.Len(t, workspaces.Items, 1)
	assert.Equal(t, "team", workspaces.Items[0].Name)
}

func TestWorkspaceGenerateName(t *testing.T) {
	t.Parallel()

	instance, err := kcptc.Run(t.Context(), kcptc.Options{Image: image})
	require.NoError(t, err)

	t.Log("Create a workspace with a generate name")
	path, err := instance.CreateWorkspace(t.Context(), "root:with:parents:gen-")
	require.NoError(t, err)
	assert.Regexp(t, `^root:with:parents:gen-.+`, path.String())

	t.Logf("Access workspace with generate name %q", path)
	cl, err := instance.Client(path.String(), client.Options{})
	require.NoError(t, err)
	require.NoError(t, cl.List(t.Context(), &tenancyv1alpha1.WorkspaceList{}), "generated workspace must be usable by returned path")
}
