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
	"github.com/testcontainers/testcontainers-go"

	kcptc "github.com/kcp-dev/kcp-testcontainers"
	"github.com/kcp-dev/kcp-testcontainers/tests/e2e/framework"
)

const labelKey = "kcp-testcontainers.e2e/component"

func TestCustomizers(t *testing.T) {
	t.Parallel()

	label := func(component string) []testcontainers.ContainerCustomizer {
		return []testcontainers.ContainerCustomizer{
			testcontainers.WithLabels(map[string]string{labelKey: component}),
		}
	}

	t.Log("Run a private kcp instance with container customizers")
	instance, err := kcptc.Run(t.Context(), kcptc.Options{
		Image:      framework.Image,
		Name:       "customizers",
		Cache:      label(kcptc.AliasCache),
		FrontProxy: label(kcptc.AliasFrontProxy),
		ShardA:     label(kcptc.ShardA),
		ShardB:     label(kcptc.ShardB),
	})
	require.NoError(t, err)

	t.Log("Validate each customizer was applied")
	containers := map[string]testcontainers.Container{
		kcptc.AliasCache:      instance.Cache,
		kcptc.AliasFrontProxy: instance.FrontProxy,
		kcptc.ShardA:          instance.ShardA,
		kcptc.ShardB:          instance.ShardB,
	}
	for component, container := range containers {
		inspect, err := container.Inspect(t.Context())
		require.NoError(t, err)
		assert.Equal(t, component, inspect.Config.Labels[labelKey], "customizer of %s must apply to its container", component)
	}
}
