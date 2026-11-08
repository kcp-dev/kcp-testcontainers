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

// Package framework provides the kcp instances shared by the e2e tests.
package framework

import (
	"cmp"
	"context"
	"os"

	kcptc "github.com/kcp-dev/kcp-testcontainers"
)

// tag is the kcp release under test.
var tag = cmp.Or(os.Getenv("KCP_VERSION"), kcptc.DefaultVersion)

// Image is the image used by the e2e tests.
//
// The e2e tests run against different releases of kcp to ensure
// backwards compatibility for a few versions.
// In CI KCP_VERSION is set to the various supported releases.
var Image = kcptc.DefaultImage + ":" + tag

// ver is the parsed KCP_VERSION.
var ver = mustParseVersion(tag)

// Before returns true when the given major/minor is before the tested kcp version.
func Before(major, minor int) bool {
	return ver.before(major, minor)
}

// After returns true when the given major/minor is after the tested kcp version.
func After(major, minor int) bool {
	return ver.after(major, minor)
}

// Default returns the instance all e2e tests share.
func Default(ctx context.Context) (*kcptc.Instance, error) {
	return kcptc.Shared(ctx, "default", kcptc.Options{
		Image: Image,
	})
}

// Second returns an instance independent of Default.
func Second(ctx context.Context) (*kcptc.Instance, error) {
	return kcptc.Shared(ctx, "second", kcptc.Options{
		Image: Image,
	})
}
