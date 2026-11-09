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
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/client-go/tools/clientcmd"
)

func TestPKI(t *testing.T) {
	t.Parallel()

	p, err := newPKI()
	require.NoError(t, err)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(p.caPEM))

	t.Run("serverCert verifies against the CA for every SAN", func(t *testing.T) {
		t.Parallel()

		kp, err := p.serverCert(ShardA, "localhost")
		require.NoError(t, err)
		verify(t, kp, pool, ShardA)
		verify(t, kp, pool, "localhost")
	})

	t.Run("clientCert carries name and groups", func(t *testing.T) {
		t.Parallel()

		kp, err := p.clientCert("kcp-admin", groupKcpAdmin)
		require.NoError(t, err)
		cert := verify(t, kp, pool, "")
		assert.Equal(t, "kcp-admin", cert.Subject.CommonName)
		assert.Equal(t, []string{groupKcpAdmin}, cert.Subject.Organization)
	})

	t.Run("service account key pair is a valid key pair", func(t *testing.T) {
		t.Parallel()

		_, err := tls.X509KeyPair(p.serviceAccount.cert, p.serviceAccount.key)
		require.NoError(t, err)
	})

	t.Run("kubeconfigBytes targets server with inline CA", func(t *testing.T) {
		t.Parallel()

		kp, err := p.clientCert("kcp-admin", groupKcpAdmin)
		require.NoError(t, err)
		raw, err := kubeconfigBytes("https://localhost:12345/clusters/root", p.caPEM, kp)
		require.NoError(t, err)

		cfg, err := clientcmd.Load(raw)
		require.NoError(t, err)
		cluster := cfg.Clusters[cfg.Contexts[cfg.CurrentContext].Cluster]
		require.NotNil(t, cluster)
		assert.Equal(t, "https://localhost:12345/clusters/root", cluster.Server)
		assert.Equal(t, p.caPEM, cluster.CertificateAuthorityData)
		assert.Contains(t, cfg.Contexts, "shard-base")
	})
}

// verify checks kp is a consistent key pair chaining to pool
// and returns the parsed leaf certificate.
func verify(t *testing.T, kp keyPair, pool *x509.CertPool, dnsName string) *x509.Certificate {
	t.Helper()

	_, err := tls.X509KeyPair(kp.cert, kp.key)
	require.NoError(t, err)

	block, _ := pem.Decode(kp.cert)
	require.NotNil(t, block)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	_, err = cert.Verify(x509.VerifyOptions{
		Roots:     pool,
		DNSName:   dnsName,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	})
	require.NoError(t, err)
	return cert
}
