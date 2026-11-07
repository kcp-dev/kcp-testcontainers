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

// Package e2e tests kcp-testcontainers.
package e2e

import (
	"cmp"
	"os"

	kcptc "github.com/kcp-dev/kcp-testcontainers"
)

// image is the image used by the e2e tests.
//
// The e2e tests run against different releases of kcp to ensure
// backwards compatibility for a few versions.
// In CI KCP_VERSION is set to the various supported releases.
var image = kcptc.DefaultImage + ":" + cmp.Or(os.Getenv("KCP_VERSION"), kcptc.DefaultVersion)
