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
)

func TestNewSharded(t *testing.T) {
	t.Parallel()

	s, err := newSharded(&Options{})
	require.NoError(t, err)

	assert.Equal(t, DefaultTag, s.opts.Image, "options must be validated")
	assert.NotNil(t, s.pki)
	assert.NotEmpty(t, s.admin.cert)
}

func TestSharded_requests(t *testing.T) {
	t.Parallel()

	s, err := newSharded(&Options{})
	require.NoError(t, err)

	t.Run("cacheRequest", func(t *testing.T) {
		t.Parallel()

		req, err := s.cacheRequest()
		require.NoError(t, err)

		assert.Equal(t, []string{"/cache-server"}, req.Entrypoint)
		assert.Contains(t, req.Cmd, "--client-ca-file="+caPath)
		assert.Contains(t, req.Cmd, "--secure-port="+portCache)
		assert.Equal(t, []string{portCache + "/tcp"}, req.ExposedPorts)
		assert.ElementsMatch(t, []string{caPath, servingCertPath, servingKeyPath}, filePaths(req))
		assert.NotNil(t, req.WaitingFor)
	})

	t.Run("shardRequest root shard has no root shard kubeconfig", func(t *testing.T) {
		t.Parallel()

		req, err := s.shardRequest(ShardA)
		require.NoError(t, err)

		assert.Equal(t, []string{"/kcp"}, req.Entrypoint)
		assert.Equal(t, "start", req.Cmd[0])
		assert.Contains(t, req.Cmd, "--shard-name="+ShardA)
		assert.Contains(t, req.Cmd, "--shard-base-url=https://"+ShardA+":"+portShard)
		assert.Contains(t, req.Cmd, "--shard-external-url=https://"+AliasFrontProxy+":"+portFrontProxy)
		assert.Contains(t, req.Cmd, "--cache-kubeconfig="+cacheKubeconfigPath)
		assert.NotContains(t, req.Cmd, "--root-shard-kubeconfig-file="+rootShardKubeconfigPath)
		assert.NotContains(t, filePaths(req), rootShardKubeconfigPath)
		assert.Equal(t, []string{portShard + "/tcp"}, req.ExposedPorts)
	})

	t.Run("shardRequest secondary shard joins through root shard", func(t *testing.T) {
		t.Parallel()

		req, err := s.shardRequest(ShardB)
		require.NoError(t, err)

		assert.Contains(t, req.Cmd, "--shard-name="+ShardB)
		assert.Contains(t, req.Cmd, "--root-shard-kubeconfig-file="+rootShardKubeconfigPath)
		assert.Contains(t, filePaths(req), rootShardKubeconfigPath)
	})

	t.Run("frontProxyRequest", func(t *testing.T) {
		t.Parallel()

		req, err := s.frontProxyRequest()
		require.NoError(t, err)

		assert.Equal(t, []string{"/kcp-front-proxy"}, req.Entrypoint)
		assert.Contains(t, req.Cmd, "--mapping-file="+mappingPath)
		assert.Contains(t, req.Cmd, "--root-kubeconfig="+rootKubeconfigPath)
		assert.Contains(t, req.Cmd, "--shards-kubeconfig="+shardsKubeconfigPath)
		assert.Equal(t, []string{portFrontProxy + "/tcp"}, req.ExposedPorts)
		assert.Subset(t, filePaths(req), []string{
			mappingPath, rootKubeconfigPath, shardsKubeconfigPath,
			requestHeaderClientCertPath, requestHeaderClientKeyPath,
		})
	})
}

func filePaths(req *testcontainers.GenericContainerRequest) []string {
	paths := make([]string, 0, len(req.Files))
	for _, f := range req.Files {
		paths = append(paths, f.ContainerFilePath)
	}
	return paths
}
