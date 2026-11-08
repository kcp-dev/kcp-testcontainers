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
	"bytes"
	"context"
	"io"
	"io/fs"
	"maps"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestNewSharded(t *testing.T) {
	t.Parallel()

	s, err := newSharded(t.Name(), &Options{})
	require.NoError(t, err)

	assert.Equal(t, DefaultTag, s.opts.Image, "options must be validated")
	assert.NotNil(t, s.pki)
}

func TestSharded_requests(t *testing.T) {
	t.Parallel()

	s, err := newSharded(t.Name(), &Options{})
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
			adminCertPath, adminKeyPath, shardAdminCertPath, shardAdminKeyPath,
		})
	})
}

func TestReadCredentials(t *testing.T) {
	t.Parallel()

	s, err := newSharded(t.Name(), &Options{})
	require.NoError(t, err)
	req, err := s.frontProxyRequest()
	require.NoError(t, err)
	stored := fileContents(t, req)

	cases := map[string]string{
		"loads stored credentials":                "",
		"reports missing CA":                      caPath,
		"reports missing admin certificate":       adminCertPath,
		"reports missing admin key":               adminKeyPath,
		"reports missing shard admin certificate": shardAdminCertPath,
		"reports missing shard admin key":         shardAdminKeyPath,
	}
	for title, missing := range cases {
		t.Run(title, func(t *testing.T) {
			t.Parallel()

			files := maps.Clone(stored)
			delete(files, missing)
			copyFile := func(_ context.Context, path string) (io.ReadCloser, error) {
				content, ok := files[path]
				if !ok {
					return nil, fs.ErrNotExist
				}
				return io.NopCloser(bytes.NewReader(content)), nil
			}

			credentials, err := readCredentials(t.Context(), copyFile)
			if missing != "" {
				require.ErrorIs(t, err, fs.ErrNotExist)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, s.pki.caPEM, credentials.ca)
			assert.Equal(t, stored[adminCertPath], credentials.admin.cert)
			assert.Equal(t, stored[adminKeyPath], credentials.admin.key)
			assert.Equal(t, stored[shardAdminCertPath], credentials.shardAdmin.cert)
			assert.Equal(t, stored[shardAdminKeyPath], credentials.shardAdmin.key)
			assert.NotEmpty(t, credentials.admin.cert, "front-proxy request must carry the admin certificate")
			assert.NotEmpty(t, credentials.shardAdmin.cert, "front-proxy request must carry the shard admin certificate")
		})
	}
}

func TestReadCredential(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		reader   io.Reader
		want     string
		copyErr  error
		readErr  error
		closeErr error
	}{
		"reads credential": {
			reader: strings.NewReader("credential"),
			want:   "credential",
		},
		"reports copy failure": {
			copyErr: fs.ErrNotExist,
		},
		"reports read failure": {
			reader:  iotest.ErrReader(io.ErrUnexpectedEOF),
			readErr: io.ErrUnexpectedEOF,
		},
		"reports close failure": {
			reader:   strings.NewReader("credential"),
			closeErr: io.ErrClosedPipe,
		},
		"preserves read and close failures": {
			reader:   iotest.ErrReader(io.ErrUnexpectedEOF),
			readErr:  io.ErrUnexpectedEOF,
			closeErr: io.ErrClosedPipe,
		},
	}
	for title, cas := range cases {
		t.Run(title, func(t *testing.T) {
			t.Parallel()

			closed := false
			copyFile := func(ctx context.Context, path string) (io.ReadCloser, error) {
				assert.Equal(t, t.Context(), ctx, "copy must use caller context")
				assert.Equal(t, adminKeyPath, path)
				if cas.copyErr != nil {
					return nil, cas.copyErr
				}
				return &credentialReadCloser{
					Reader: cas.reader,
					close: func() error {
						closed = true
						return cas.closeErr
					},
				}, nil
			}

			content, err := readCredential(t.Context(), copyFile, adminKeyPath)
			if cas.copyErr != nil || cas.readErr != nil || cas.closeErr != nil {
				require.Error(t, err)
				assert.Nil(t, content, "failed reads must not expose partial credentials")
			} else {
				require.NoError(t, err)
				assert.Equal(t, cas.want, string(content))
			}
			for _, expected := range []error{cas.copyErr, cas.readErr, cas.closeErr} {
				if expected != nil {
					require.ErrorIs(t, err, expected)
				}
			}
			assert.Equal(t, cas.copyErr == nil, closed, "every opened credential reader must be closed")
		})
	}
}

type credentialReadCloser struct {
	io.Reader
	close func() error
}

func (reader *credentialReadCloser) Close() error {
	return reader.close()
}

func fileContents(t *testing.T, req *testcontainers.GenericContainerRequest) map[string][]byte {
	t.Helper()

	files := make(map[string][]byte, len(req.Files))
	for _, f := range req.Files {
		require.NotNil(t, f.Reader)
		content, err := io.ReadAll(f.Reader)
		require.NoError(t, err)
		files[f.ContainerFilePath] = content
	}
	return files
}

func TestSharded_featureGateArgs(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		gates []string
		want  []string
	}{
		"no gates omits the flag": {},
		"single gate": {
			gates: []string{"WorkspaceMounts=true"},
			want:  []string{"--feature-gates=WorkspaceMounts=true"},
		},
		"multiple gates are comma separated": {
			gates: []string{"WorkspaceMounts=false", "CacheAPIs=true"},
			want:  []string{"--feature-gates=WorkspaceMounts=false,CacheAPIs=true"},
		},
	}
	for title, cas := range cases {
		t.Run(title, func(t *testing.T) {
			t.Parallel()

			s, err := newSharded(t.Name(), &Options{FeatureGates: cas.gates})
			require.NoError(t, err)

			assert.Equal(t, cas.want, s.featureGateArgs())

			shard, err := s.shardRequest(ShardA)
			require.NoError(t, err)
			frontProxy, err := s.frontProxyRequest()
			require.NoError(t, err)
			cache, err := s.cacheRequest()
			require.NoError(t, err)
			for _, want := range cas.want {
				assert.Contains(t, shard.Cmd, want)
				assert.Contains(t, frontProxy.Cmd, want)
			}
			for _, arg := range cache.Cmd {
				assert.NotContains(t, arg, "--feature-gates", "cache-server has no feature gates")
			}
		})
	}
}

func filePaths(req *testcontainers.GenericContainerRequest) []string {
	paths := make([]string, 0, len(req.Files))
	for _, f := range req.Files {
		paths = append(paths, f.ContainerFilePath)
	}
	return paths
}
