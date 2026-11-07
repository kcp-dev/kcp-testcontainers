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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	tcwait "github.com/testcontainers/testcontainers-go/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/rest"

	corev1alpha1 "github.com/kcp-dev/sdk/apis/core/v1alpha1"
	kcptestingserver "github.com/kcp-dev/sdk/testing/server"
)

// shardPollInterval is how often the Shard objects are listed during startup.
const shardPollInterval = time.Second

// Network aliases of the components.
const (
	AliasCache      = "cache"
	AliasFrontProxy = "front-proxy"
	ShardA          = "root"
	ShardB          = "shard-b"
)

// Groups kcp grants built-in permissions to.
// These are only in the kcp root module.
const (
	groupKcpAdmin                    = "system:kcp:admin"
	groupLogicalClusterAdmin         = "system:kcp:logical-cluster-admin"
	groupExternalLogicalClusterAdmin = "system:kcp:external-logical-cluster-admin"
	groupSystemMasters               = user.SystemPrivilegedGroup
)

// In-network ports of the components.
const (
	portFrontProxy = "6443"
	portShard      = "6444"
	portCache      = "8012"
)

// Paths inside the containers.
const (
	// rootDirectory is the working directory of the kcp process in the container.
	rootDirectory = "/.kcp"

	// pkiDir is the root dir for all x509-related files in the container.
	pkiDir = "/.kcp-pki"
	caPath = pkiDir + "/ca.crt"

	// The admin credentials for other processes to read when sharing an instance.
	adminCertPath      = pkiDir + "/admin.crt"
	adminKeyPath       = pkiDir + "/admin.key"
	shardAdminCertPath = pkiDir + "/shard-admin.crt"
	shardAdminKeyPath  = pkiDir + "/shard-admin.key"

	serviceAccountCertPath = pkiDir + "/service-account.crt"
	serviceAccountKeyPath  = pkiDir + "/service-account.key"

	servingCertPath = pkiDir + "/apiserver.crt"
	servingKeyPath  = pkiDir + "/apiserver.key"

	shardClientCertPath = pkiDir + "/shard-client.crt"
	shardClientKeyPath  = pkiDir + "/shard-client.key"

	requestHeaderClientCertPath = pkiDir + "/requestheader.crt"
	requestHeaderClientKeyPath  = pkiDir + "/requestheader.key"

	cacheKubeconfigPath                       = pkiDir + "/cache.kubeconfig"
	rootShardKubeconfigPath                   = pkiDir + "/root-shard.kubeconfig"
	rootKubeconfigPath                        = pkiDir + "/root.kubeconfig"
	shardsKubeconfigPath                      = pkiDir + "/shards.kubeconfig"
	logicalClusterAdminKubeconfigPath         = pkiDir + "/logical-cluster-admin.kubeconfig"
	externalLogicalClusterAdminKubeconfigPath = pkiDir + "/external-logical-cluster-admin.kubeconfig"

	mappingPath = pkiDir + "/mapping.yaml"
)

// sharded wires pki and Options into the topology and runs it.
type sharded struct {
	pki  *pki
	opts *Options

	// admin is the kcp-admin client certificate of the returned rest.Config.
	admin keyPair

	// shardAdmin is the system:masters client certificate for direct shard access.
	shardAdmin keyPair

	network    *testcontainers.DockerNetwork
	cache      testcontainers.Container
	frontProxy testcontainers.Container
	shardA     testcontainers.Container
	shardB     testcontainers.Container

	base *rest.Config
}

// newSharded creates the topology with a fresh pki.
func newSharded(opts *Options) (*sharded, error) {
	if err := opts.validate(); err != nil {
		return nil, fmt.Errorf("invalid options: %w", err)
	}

	p, err := newPKI()
	if err != nil {
		return nil, err
	}

	admin, err := p.clientCert("kcp-admin", groupKcpAdmin)
	if err != nil {
		return nil, err
	}

	shardAdmin, err := p.clientCert("shard-admin", groupSystemMasters)
	if err != nil {
		return nil, err
	}

	return &sharded{
		pki:        p,
		opts:       opts,
		admin:      admin,
		shardAdmin: shardAdmin,
	}, nil
}

func (s *sharded) start(ctx context.Context) error {
	var err error
	s.network, err = tcnetwork.New(ctx)
	if err != nil {
		return fmt.Errorf("creating network: %w", err)
	}

	s.cache, err = s.startContainer(ctx, AliasCache, s.cacheRequest)
	if err != nil {
		return err
	}

	s.shardA, err = s.startContainer(ctx, ShardA, func() (*testcontainers.GenericContainerRequest, error) {
		return s.shardRequest(ShardA)
	})
	if err != nil {
		return err
	}

	s.shardB, err = s.startContainer(ctx, ShardB, func() (*testcontainers.GenericContainerRequest, error) {
		return s.shardRequest(ShardB)
	})
	if err != nil {
		return err
	}

	s.frontProxy, err = s.startContainer(ctx, AliasFrontProxy, s.frontProxyRequest)
	if err != nil {
		return err
	}

	if err := s.loadCredentials(ctx); err != nil {
		return err
	}

	cfg := rest.CopyConfig(s.base)
	cfg.Host += "/clusters/" + ShardA

	readyCtx, cancel := context.WithTimeout(ctx, s.opts.StartupTimeout)
	defer cancel()

	if err := waitForShards(readyCtx, cfg); err != nil {
		return fmt.Errorf("waiting for shard to be registered: %w", err)
	}

	if err := kcptestingserver.WaitForReady(readyCtx, cfg); err != nil {
		return fmt.Errorf("waiting for instance to be ready: %w", err)
	}

	return nil
}

// startContainer builds the request for alias, attaches it to the network, applies the customizers of alias and starts it.
func (s *sharded) startContainer(ctx context.Context, alias string, build func() (*testcontainers.GenericContainerRequest, error)) (testcontainers.Container, error) {
	req, err := build()
	if err != nil {
		return nil, err
	}

	req.Networks = []string{s.network.Name}
	req.NetworkAliases = map[string][]string{
		s.network.Name: {alias},
	}
	req.Started = true
	for _, customizer := range s.opts.customizers(alias) {
		if err := customizer.Customize(req); err != nil {
			return nil, fmt.Errorf("customizing %s: %w", alias, err)
		}
	}

	container, err := testcontainers.GenericContainer(ctx, *req)
	if err != nil {
		// a failed start may still have created the container.
		return container, fmt.Errorf("starting %s: %w", alias, err)
	}

	return container, nil
}

func waitForShards(ctx context.Context, cfg *rest.Config) error {
	cl, err := client.New(cfg, client.Options{
		Scheme: defaultScheme(),
	})
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	if err := wait.PollUntilContextCancel(ctx, shardPollInterval, true,
		func(ctx context.Context) (bool, error) {
			shards := &corev1alpha1.ShardList{}
			if err := cl.List(ctx, shards); err != nil {
				return false, nil //nolint:nilerr // front-proxy may still 503 while shards register
			}
			return len(shards.Items) == 2, nil
		},
	); err != nil {
		return fmt.Errorf("waiting for %s and %s to register: %w", ShardA, ShardB, err)
	}

	return nil
}

func (s *sharded) loadCredentials(ctx context.Context) error {
	credentials, err := readCredentials(ctx, s.frontProxy.CopyFileFromContainer)
	if err != nil {
		return err
	}

	address, err := mappedAddress(ctx, s.frontProxy, portFrontProxy)
	if err != nil {
		return fmt.Errorf("resolving front-proxy address: %w", err)
	}

	s.base = &rest.Config{
		Host: "https://" + address,
		TLSClientConfig: rest.TLSClientConfig{
			ServerName: AliasFrontProxy,
			CAData:     credentials.ca,
			CertData:   credentials.admin.cert,
			KeyData:    credentials.admin.key,
		},
	}
	s.admin = credentials.admin
	s.shardAdmin = credentials.shardAdmin
	return nil
}

type clientCredentials struct {
	ca         []byte
	admin      keyPair
	shardAdmin keyPair
}

func readCredentials(ctx context.Context, copyFile func(context.Context, string) (io.ReadCloser, error)) (clientCredentials, error) {
	caFile, err := readCredential(ctx, copyFile, caPath)
	if err != nil {
		return clientCredentials{}, err
	}

	adminCertFile, err := readCredential(ctx, copyFile, adminCertPath)
	if err != nil {
		return clientCredentials{}, err
	}

	adminKeyFile, err := readCredential(ctx, copyFile, adminKeyPath)
	if err != nil {
		return clientCredentials{}, err
	}

	shardAdminCertFile, err := readCredential(ctx, copyFile, shardAdminCertPath)
	if err != nil {
		return clientCredentials{}, err
	}

	shardAdminKeyFile, err := readCredential(ctx, copyFile, shardAdminKeyPath)
	if err != nil {
		return clientCredentials{}, err
	}

	credentials := clientCredentials{
		ca: caFile,
		admin: keyPair{
			cert: adminCertFile,
			key:  adminKeyFile,
		},
		shardAdmin: keyPair{
			cert: shardAdminCertFile,
			key:  shardAdminKeyFile,
		},
	}

	return credentials, nil
}

func readCredential(ctx context.Context, copyFile func(context.Context, string) (io.ReadCloser, error), path string) ([]byte, error) {
	reader, err := copyFile(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("copying credential %s: %w", path, err)
	}
	content, readErr := io.ReadAll(reader)
	if err := errors.Join(readErr, reader.Close()); err != nil {
		return nil, fmt.Errorf("reading credential %s: %w", path, err)
	}
	return content, nil
}

// waitReadyz retunrs the testcontainer wait strategy probing /readyz.
func (s *sharded) waitReadyz(port string) tcwait.Strategy {
	return tcwait.ForHTTP("/readyz").
		WithPort(port+"/tcp").
		WithTLS(true, &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // readiness probe against our own CA
		}).
		WithStartupTimeout(s.opts.StartupTimeout)
}

// file wraps content as a container file at path.
// Mode is world-readable because the image runs as 65532 and files are copied as root.
func file(path string, content []byte) testcontainers.ContainerFile {
	return testcontainers.ContainerFile{
		Reader:            bytes.NewReader(content),
		ContainerFilePath: path,
		FileMode:          0o644,
	}
}

// inNetworkURL returns the https URL of alias on its in-network port.
func inNetworkURL(alias, port string) string {
	return "https://" + alias + ":" + port
}

// frontProxyRequest builds the kcp-front-proxy container request.
func (s *sharded) frontProxyRequest() (*testcontainers.GenericContainerRequest, error) {
	serving, err := s.pki.serverCert(AliasFrontProxy, "localhost")
	if err != nil {
		return nil, err
	}

	requestHeaderClient, err := s.pki.clientCert("kcp-front-proxy")
	if err != nil {
		return nil, err
	}

	shardAdmin, err := s.pki.clientCert("shard-admin", groupSystemMasters)
	if err != nil {
		return nil, err
	}

	rootKubeconfig, err := kubeconfigBytes(inNetworkURL(ShardA, portShard), s.pki.caPEM, shardAdmin)
	if err != nil {
		return nil, err
	}

	shardsKubeconfig, err := kubeconfigBytes("", s.pki.caPEM, shardAdmin)
	if err != nil {
		return nil, err
	}

	mapping := fmt.Sprintf(`- path: /clusters/
  backend: %s
  backend_server_ca: %s
  proxy_client_cert: %s
  proxy_client_key: %s
`, inNetworkURL(ShardA, portShard), caPath, requestHeaderClientCertPath, requestHeaderClientKeyPath)

	return &testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:      s.opts.Image,
			Entrypoint: []string{"/kcp-front-proxy"},
			Cmd: []string{
				"--root-directory=" + rootDirectory,
				"--secure-port=" + portFrontProxy,
				"--mapping-file=" + mappingPath,
				"--root-kubeconfig=" + rootKubeconfigPath, // TODO(ntnn): deprecated, keep for backwards compat
				"--shards-kubeconfig=" + shardsKubeconfigPath,
				"--client-ca-file=" + caPath,
				"--tls-cert-file=" + servingCertPath,
				"--tls-private-key-file=" + servingKeyPath,
			},
			Files: []testcontainers.ContainerFile{
				file(caPath, s.pki.caPEM),
				file(servingCertPath, serving.cert),
				file(servingKeyPath, serving.key),
				file(requestHeaderClientCertPath, requestHeaderClient.cert),
				file(requestHeaderClientKeyPath, requestHeaderClient.key),
				file(adminCertPath, s.admin.cert),
				file(adminKeyPath, s.admin.key),
				file(shardAdminCertPath, s.shardAdmin.cert),
				file(shardAdminKeyPath, s.shardAdmin.key),
				file(rootKubeconfigPath, rootKubeconfig),
				file(shardsKubeconfigPath, shardsKubeconfig),
				file(mappingPath, []byte(mapping)),
			},
			ExposedPorts: []string{portFrontProxy + "/tcp"},
			WaitingFor:   s.waitReadyz(portFrontProxy),
		},
	}, nil
}

// shardRequest builds the kcp shard container request for alias.
//
// TODO(ntnn): ShardA is implicitly the root shard.
func (s *sharded) shardRequest(alias string) (*testcontainers.GenericContainerRequest, error) {
	serving, err := s.pki.serverCert(alias, "localhost")
	if err != nil {
		return nil, err
	}

	shardClient, err := s.pki.clientCert("kcp-shard-"+alias, groupSystemMasters)
	if err != nil {
		return nil, err
	}

	cacheClient, err := s.pki.clientCert("cache-client", groupSystemMasters)
	if err != nil {
		return nil, err
	}

	logicalClusterAdmin, err := s.pki.clientCert("logical-cluster-admin", groupLogicalClusterAdmin)
	if err != nil {
		return nil, err
	}

	externalLogicalClusterAdmin, err := s.pki.clientCert("external-logical-cluster-admin", groupExternalLogicalClusterAdmin)
	if err != nil {
		return nil, err
	}

	cacheKubeconfig, err := kubeconfigBytes(inNetworkURL(AliasCache, portCache), s.pki.caPEM, cacheClient)
	if err != nil {
		return nil, err
	}

	logicalClusterAdminKubeconfig, err := kubeconfigBytes(inNetworkURL(ShardA, portShard), s.pki.caPEM, logicalClusterAdmin)
	if err != nil {
		return nil, err
	}

	externalLogicalClusterAdminKubeconfig, err := kubeconfigBytes(inNetworkURL(AliasFrontProxy, portFrontProxy), s.pki.caPEM, externalLogicalClusterAdmin)
	if err != nil {
		return nil, err
	}

	cmd := []string{
		"start",
		"--shard-name=" + alias,
		"--root-directory=" + rootDirectory,
		"--bind-address=0.0.0.0",
		"--secure-port=" + portShard,
		"--shard-base-url=" + inNetworkURL(alias, portShard),
		"--shard-external-url=" + inNetworkURL(AliasFrontProxy, portFrontProxy),
		"--external-hostname=" + AliasFrontProxy,
		"--client-ca-file=" + caPath,
		"--requestheader-client-ca-file=" + caPath,
		"--requestheader-allowed-names=kcp-front-proxy",
		"--requestheader-username-headers=X-Remote-User",
		"--requestheader-group-headers=X-Remote-Group",
		"--requestheader-extra-headers-prefix=X-Remote-Extra-",
		"--service-account-key-file=" + serviceAccountCertPath,
		"--service-account-private-key-file=" + serviceAccountKeyPath,
		"--service-account-signing-key-file=" + serviceAccountKeyPath,
		"--tls-cert-file=" + servingCertPath,
		"--tls-private-key-file=" + servingKeyPath,
		"--shard-client-cert-file=" + shardClientCertPath,
		"--shard-client-key-file=" + shardClientKeyPath,
		"--shard-virtual-workspace-ca-file=" + caPath,
		"--cache-kubeconfig=" + cacheKubeconfigPath,
		"--logical-cluster-admin-kubeconfig=" + logicalClusterAdminKubeconfigPath,
		"--external-logical-cluster-admin-kubeconfig=" + externalLogicalClusterAdminKubeconfigPath,
	}

	files := []testcontainers.ContainerFile{
		file(caPath, s.pki.caPEM),
		file(serviceAccountCertPath, s.pki.serviceAccount.cert),
		file(serviceAccountKeyPath, s.pki.serviceAccount.key),
		file(servingCertPath, serving.cert),
		file(servingKeyPath, serving.key),
		file(shardClientCertPath, shardClient.cert),
		file(shardClientKeyPath, shardClient.key),
		file(cacheKubeconfigPath, cacheKubeconfig),
		file(logicalClusterAdminKubeconfigPath, logicalClusterAdminKubeconfig),
		file(externalLogicalClusterAdminKubeconfigPath, externalLogicalClusterAdminKubeconfig),
	}

	if alias != ShardA {
		shardAdmin, err := s.pki.clientCert("shard-admin", groupSystemMasters)
		if err != nil {
			return nil, err
		}

		rootShardKubeconfig, err := kubeconfigBytes(inNetworkURL(ShardA, portShard), s.pki.caPEM, shardAdmin)
		if err != nil {
			return nil, err
		}

		cmd = append(cmd, "--root-shard-kubeconfig-file="+rootShardKubeconfigPath)
		files = append(files, file(rootShardKubeconfigPath, rootShardKubeconfig))
	}

	return &testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        s.opts.Image,
			Entrypoint:   []string{"/kcp"},
			Cmd:          cmd,
			Files:        files,
			ExposedPorts: []string{portShard + "/tcp"},
			WaitingFor:   s.waitReadyz(portShard),
		},
	}, nil
}

// cacheRequest builds the cache-server container request.
func (s *sharded) cacheRequest() (*testcontainers.GenericContainerRequest, error) {
	serving, err := s.pki.serverCert(AliasCache, "localhost")
	if err != nil {
		return nil, err
	}

	return &testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:      s.opts.Image,
			Entrypoint: []string{"/cache-server"},
			Cmd: []string{
				"--root-directory=" + rootDirectory,
				"--bind-address=0.0.0.0",
				"--secure-port=" + portCache,
				"--client-ca-file=" + caPath,
				"--tls-cert-file=" + servingCertPath,
				"--tls-private-key-file=" + servingKeyPath,
			},
			Files: []testcontainers.ContainerFile{
				file(caPath, s.pki.caPEM),
				file(servingCertPath, serving.cert),
				file(servingKeyPath, serving.key),
			},
			ExposedPorts: []string{portCache + "/tcp"},
			WaitingFor:   s.waitReadyz(portCache),
		},
	}, nil
}
