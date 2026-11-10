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
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/kcp-dev/sdk/testing/third_party/library-go/crypto"
)

const (
	caName              = "kcp-ca"
	requestHeaderCAName = "kcp-front-proxy-requestheader-ca"
	serviceAccountName  = "kcp-service-account-signing-key"

	// 365 days, more than enough for a short-live test instance.
	certLifetimeDays = 365
	certLifetime     = certLifetimeDays * 24 * time.Hour
)

type keyPair struct {
	cert []byte
	key  []byte
}

// pki is a CA used to sign every serving and client certificate of
// a kcptc instance and the service account signing key of all shards.
//
// The front-proxy's request-header client certificate is signed by a
// CA of its own: the shards trust it only for the X-Remote-* headers.
// Were it signed by the client CA, a request the front-proxy passes
// through unauthenticated (e.g. with a service account token) would
// be authenticated by the shard as the front-proxy itself.
type pki struct {
	ca    *crypto.CA
	caPEM []byte

	requestHeaderCA    *crypto.CA
	requestHeaderCAPEM []byte

	serviceAccount keyPair
}

func newPKI() (*pki, error) {
	ca, caPEM, err := newCA(caName)
	if err != nil {
		return nil, err
	}
	requestHeaderCA, requestHeaderCAPEM, err := newCA(requestHeaderCAName)
	if err != nil {
		return nil, err
	}

	serviceAccount, err := selfSignedKeyPair(serviceAccountName)
	if err != nil {
		return nil, err
	}

	return &pki{
		ca:                 ca,
		caPEM:              caPEM,
		requestHeaderCA:    requestHeaderCA,
		requestHeaderCAPEM: requestHeaderCAPEM,
		serviceAccount:     serviceAccount,
	}, nil
}

func newCA(name string) (*crypto.CA, []byte, error) {
	cfg, err := crypto.MakeSelfSignedCAConfig(name, certLifetimeDays)
	if err != nil {
		return nil, nil, fmt.Errorf("creating CA %q: %w", name, err)
	}
	certPEM, _, err := cfg.GetPEMBytes()
	if err != nil {
		return nil, nil, fmt.Errorf("encoding CA %q: %w", name, err)
	}
	return &crypto.CA{
		Config:          cfg,
		SerialGenerator: &crypto.RandomSerialGenerator{},
	}, certPEM, nil
}

// serverCert issues a serving certificate with hostnames as SANs.
func (p *pki) serverCert(hostnames ...string) (keyPair, error) {
	cfg, err := p.ca.MakeServerCertForDuration(sets.New(hostnames...), certLifetime)
	if err != nil {
		return keyPair{}, fmt.Errorf("creating serving certificate for %v: %w", hostnames, err)
	}
	return toKeyPair(cfg)
}

// requestHeaderClientCert issues the front-proxy's client certificate
// for the request-header authentication of the shards.
func (p *pki) requestHeaderClientCert(name string) (keyPair, error) {
	cfg, err := p.requestHeaderCA.MakeClientCertificateForDuration(&user.DefaultInfo{Name: name}, certLifetime)
	if err != nil {
		return keyPair{}, fmt.Errorf("creating request-header client certificate %q: %w", name, err)
	}
	return toKeyPair(cfg)
}

// clientCert issues a client certificate for name in groups.
func (p *pki) clientCert(name string, groups ...string) (keyPair, error) {
	cfg, err := p.ca.MakeClientCertificateForDuration(
		&user.DefaultInfo{
			Name:   name,
			Groups: groups,
		},
		certLifetime,
	)
	if err != nil {
		return keyPair{}, fmt.Errorf("creating client certificate %q: %w", name, err)
	}
	return toKeyPair(cfg)
}

// selfSignedKeyPair returns a standalone signing key pair.
func selfSignedKeyPair(name string) (keyPair, error) {
	cfg, err := crypto.MakeSelfSignedCAConfig(name, certLifetimeDays)
	if err != nil {
		return keyPair{}, fmt.Errorf("creating self-signed key pair %q: %w", name, err)
	}
	return toKeyPair(cfg)
}

func toKeyPair(cfg *crypto.TLSCertificateConfig) (keyPair, error) {
	cert, key, err := cfg.GetPEMBytes()
	if err != nil {
		return keyPair{}, fmt.Errorf("encoding certificate: %w", err)
	}
	return keyPair{
		cert: cert,
		key:  key,
	}, nil
}

// kubeconfigBytes builds a kubeconfig with inline certificate data.
// Contexts base and shard-base are the names kcp components select.
func kubeconfigBytes(server string, caPEM []byte, client keyPair) ([]byte, error) {
	cfg := clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			"cluster": {
				Server:                   server,
				CertificateAuthorityData: caPEM,
			},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"user": {
				ClientCertificateData: client.cert,
				ClientKeyData:         client.key,
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			"base": {
				Cluster:  "cluster",
				AuthInfo: "user",
			},
			"shard-base": {
				Cluster:  "cluster",
				AuthInfo: "user",
			},
		},
		CurrentContext: "base",
	}
	raw, err := clientcmd.Write(cfg)
	if err != nil {
		return nil, fmt.Errorf("serializing kubeconfig for %q: %w", server, err)
	}
	return raw, nil
}
