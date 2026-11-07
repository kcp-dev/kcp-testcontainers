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

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	apisv1alpha1 "github.com/kcp-dev/sdk/apis/apis/v1alpha1"
	apisv1alpha2 "github.com/kcp-dev/sdk/apis/apis/v1alpha2"
	kcptesting "github.com/kcp-dev/sdk/testing"

	kcptc "github.com/kcp-dev/kcp-testcontainers"
)

const (
	bindTimeout = time.Minute
	bindTick    = 100 * time.Millisecond
)

const sheriffSchema = `{
	"description": "Sheriff is part of the wild west",
	"type": "object",
	"properties": {
		"spec": {
			"type": "object",
			"properties": {"intent": {"type": "string"}}
		}
	}
}`

func TestAPIBindingAcrossShards(t *testing.T) {
	t.Parallel()

	instance, err := kcptc.Run(t.Context(), kcptc.Options{Image: image})
	require.NoError(t, err)

	t.Logf("Create a provider workspace on shard %q", kcptc.ShardA)
	providerPath, err := instance.CreateWorkspace(t.Context(), "root:provider", kcptesting.WithShard(kcptc.ShardA))
	require.NoError(t, err)
	provider, err := instance.Client(providerPath.String(), client.Options{})
	require.NoError(t, err)

	t.Logf("Create a consumer workspace on shard %q", kcptc.ShardB)
	consumerPath, err := instance.CreateWorkspace(t.Context(), "root:consumer", kcptesting.WithShard(kcptc.ShardB))
	require.NoError(t, err)
	consumer, err := instance.Client(consumerPath.String(), client.Options{})
	require.NoError(t, err)

	t.Log("Export the sheriff API in the provider workspace")
	sheriffs := &apisv1alpha1.APIResourceSchema{
		ObjectMeta: metav1.ObjectMeta{Name: "v1.sheriffs.wildwest.dev"},
		Spec: apisv1alpha1.APIResourceSchemaSpec{
			Group: "wildwest.dev",
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   "sheriffs",
				Singular: "sheriff",
				Kind:     "Sheriff",
				ListKind: "SheriffList",
			},
			Scope: apiextensionsv1.ClusterScoped,
			Versions: []apisv1alpha1.APIResourceVersion{{
				Name:    "v1alpha1",
				Served:  true,
				Storage: true,
				Schema:  runtime.RawExtension{Raw: []byte(sheriffSchema)},
			}},
		},
	}
	require.NoError(t, provider.Create(t.Context(), sheriffs))

	export := &apisv1alpha2.APIExport{
		ObjectMeta: metav1.ObjectMeta{Name: "wildwest.dev"},
		Spec: apisv1alpha2.APIExportSpec{
			Resources: []apisv1alpha2.ResourceSchema{{
				Name:   "sheriffs",
				Group:  "wildwest.dev",
				Schema: "v1.sheriffs.wildwest.dev",
				Storage: apisv1alpha2.ResourceSchemaStorage{
					CRD: &apisv1alpha2.ResourceSchemaStorageCRD{},
				},
			}},
		},
	}
	require.NoError(t, provider.Create(t.Context(), export))

	t.Log("Bind the sheriff API in the consumer workspace")
	binding := &apisv1alpha2.APIBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "wildwest.dev"},
		Spec: apisv1alpha2.APIBindingSpec{
			Reference: apisv1alpha2.BindingReference{
				Export: &apisv1alpha2.ExportBindingReference{
					Path: providerPath.String(),
					Name: "wildwest.dev",
				},
			},
		},
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.NoError(c, consumer.Create(t.Context(), binding.DeepCopy()))
	}, bindTimeout, bindTick, "APIBinding must be created")

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		current := &apisv1alpha2.APIBinding{}
		require.NoError(c, consumer.Get(t.Context(), client.ObjectKey{Name: "wildwest.dev"}, current))
		assert.Equal(c, apisv1alpha2.APIBindingPhaseBound, current.Status.Phase)
	}, bindTimeout, bindTick, "APIBinding must become bound")

	t.Log("Create a Sheriff in the consumer worksapce")
	sheriff := &unstructured.Unstructured{}
	sheriff.SetGroupVersionKind(schema.GroupVersionKind{Group: "wildwest.dev", Version: "v1alpha1", Kind: "Sheriff"})
	sheriff.SetName("woody")
	require.NoError(t, consumer.Create(t.Context(), sheriff))

	t.Log("Validate the VW URL on the APIExportES")
	var vwURL string
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		slice := &apisv1alpha1.APIExportEndpointSlice{}
		require.NoError(c, provider.Get(t.Context(), client.ObjectKey{Name: "wildwest.dev"}, slice))
		require.NotEmpty(c, slice.Status.APIExportEndpoints)
		vwURL = slice.Status.APIExportEndpoints[0].URL
	}, bindTimeout, bindTick, "APIExportEndpointSlice must publish a virtual workspace URL")

	external, err := instance.ExternalURL(t.Context(), vwURL)
	require.NoError(t, err)
	t.Logf("virtual workspace %s mapped to %s", vwURL, external)

	config := instance.Config()
	config.Host = external + "/clusters/*"
	// the VW is served by a shard whose certificate names the shard, not the front-proxy.
	config.TLSClientConfig.ServerName = ""
	config.TLSClientConfig.Insecure = true
	config.TLSClientConfig.CAData = nil
	vw, err := client.New(config, client.Options{})
	require.NoError(t, err)

	t.Log("Validate provider can see the consumer Sheriff through the VW URL")
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{Group: "wildwest.dev", Version: "v1alpha1", Kind: "SheriffList"})
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		require.NoError(c, vw.List(t.Context(), list))
		assert.Len(c, list.Items, 1)
	}, bindTimeout, bindTick, "sheriff must be visible through the virtual workspace")
	assert.Equal(t, "woody", list.Items[0].GetName())
}
