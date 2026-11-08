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
	"time"

	"github.com/testcontainers/testcontainers-go"
)

const (
	// DefaultVersion is the default kcp version deployed by kcp-testcontainers.
	DefaultVersion = "v0.33.6"
	// DefaultImage is the default image deployed by kcp-testcontainers.
	DefaultImage = "ghcr.io/kcp-dev/kcp"
	// DefaultTag is the default image tag deployed by kcp-testcontainers.
	DefaultTag = DefaultImage + ":" + DefaultVersion

	// DefaultStartupTimeout bounds waiting for the whole topology to become ready.
	DefaultStartupTimeout = 2 * time.Minute
)

// Options configures the kcp topology.
// Customizers apply to the container request of the named component.
type Options struct {
	// Image is the kcp image including tag.
	// Defaults to [DefaultTag].
	Image string

	// StartupTimeout bounds waiting for all components to become ready.
	// Defaults to [DefaultStartupTimeout].
	StartupTimeout time.Duration

	// FrontProxy is [testcontainers.ContainerCustomizer] applied to the front-proxy container after settings derived from the other options.
	FrontProxy []testcontainers.ContainerCustomizer
	// ShardA is [testcontainers.ContainerCustomizer] applied to the first shard container after settings derived from the other options.
	ShardA []testcontainers.ContainerCustomizer
	// ShardB is [testcontainers.ContainerCustomizer] applied to the second shard container after settings derived from the other options.
	ShardB []testcontainers.ContainerCustomizer
	// Cache is [testcontainers.ContainerCustomizer] applied to the cache-server container after settings derived from the other options.
	Cache []testcontainers.ContainerCustomizer
}

func (o *Options) validate() error {
	if o.Image == "" {
		o.Image = DefaultTag
	}
	if o.StartupTimeout == 0 {
		o.StartupTimeout = DefaultStartupTimeout
	}
	return nil
}

// customizers returns the customizers registered for alias.
func (o *Options) customizers(alias string) []testcontainers.ContainerCustomizer {
	switch alias {
	case AliasCache:
		return o.Cache
	case AliasFrontProxy:
		return o.FrontProxy
	case ShardA:
		return o.ShardA
	case ShardB:
		return o.ShardB
	default:
		return nil
	}
}
