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

package framework

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseVersion(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		tag     string
		want    version
		wantErr bool
	}{
		"release tag":      {tag: "v0.33.6", want: version{major: 0, minor: 33}},
		"release branch":   {tag: "release-0.31", want: version{major: 0, minor: 31}},
		"major and minor":  {tag: "1.2", want: version{major: 1, minor: 2}},
		"missing minor":    {tag: "v1", wantErr: true},
		"non-numeric":      {tag: "main", wantErr: true},
		"non-numeric part": {tag: "v0.x.1", wantErr: true},
	}
	for title, cas := range cases {
		t.Run(title, func(t *testing.T) {
			t.Parallel()

			got, err := parseVersion(cas.tag)
			if cas.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, cas.want, got)
		})
	}
}

func TestVersion_Before(t *testing.T) {
	t.Parallel()

	v := version{major: 0, minor: 32}
	cases := map[string]struct {
		major, minor int
		before       bool
		after        bool
	}{
		"older minor":  {0, 33, true, false},
		"same version": {0, 32, false, false},
		"newer minor":  {0, 31, false, true},
		"older major":  {1, 0, true, false},
	}
	for title, cas := range cases {
		t.Run(title, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, cas.before, v.before(cas.major, cas.minor), "Before")
			assert.Equal(t, cas.after, v.after(cas.major, cas.minor), "After")
		})
	}
}
