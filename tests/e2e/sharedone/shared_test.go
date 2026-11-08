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

// Package sharedone is one of three tests validating shared
// cross-process instances.
package sharedone

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"k8s.io/apimachinery/pkg/util/wait"

	tenancyv1alpha1 "github.com/kcp-dev/sdk/apis/tenancy/v1alpha1"

	"github.com/kcp-dev/kcp-testcontainers/tests/e2e/framework"
)

// TestSharedOne can only run together with TestSharedTwo and
// TestSharedThree from tests/e2e/sharedtwo/ and .../sharedthree/
// respectively.
// Each test creates one workspace (one, two, three) in the same parent
// worksapce (root:shared). Then all three wait until they can see all
// three workspaces.
//
// This validates that `framework.Second`, which uses `kcptc.Shared`,
// hands out clients to the same instance to tests in different
// processes.
func TestSharedOne(t *testing.T) {
	t.Parallel()

	instance, err := framework.Second(t.Context())
	require.NoError(t, err)

	t.Log("Creating workspace root:shared:one")
	_, err = instance.CreateWorkspace(t.Context(), "root:shared:one")
	require.NoError(t, err)

	t.Log("Get client for root:shared")
	sharedWS, err := instance.Client("root:shared", client.Options{})
	require.NoError(t, err)

	t.Log("Wait until workspaces one, two and three are present")
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		wsList := &tenancyv1alpha1.WorkspaceList{}
		require.NoError(c, sharedWS.List(t.Context(), wsList))
		names := make([]string, 0, len(wsList.Items))
		for _, ws := range wsList.Items {
			names = append(names, ws.Name)
		}
		assert.ElementsMatch(c, []string{"one", "two", "three"}, names)
	}, wait.ForeverTestTimeout, 100*time.Millisecond)
}
