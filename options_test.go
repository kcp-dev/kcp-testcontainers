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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOptions_validate(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		in   Options
		want Options
	}{
		"zero value gets defaults": {
			in: Options{},
			want: Options{
				Image:          DefaultTag,
				StartupTimeout: DefaultStartupTimeout,
			},
		},
		"explicit values kept": {
			in: Options{
				Image:          "example.com/kcp:dev",
				StartupTimeout: time.Second,
			},
			want: Options{
				Image:          "example.com/kcp:dev",
				StartupTimeout: time.Second,
			},
		},
	}

	for title, cas := range cases {
		t.Run(title, func(t *testing.T) {
			t.Parallel()

			opts := cas.in
			require.NoError(t, opts.validate())
			assert.Equal(t, cas.want, opts)
		})
	}
}
