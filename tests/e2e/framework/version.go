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
	"fmt"
	"strconv"
	"strings"
)

type version struct {
	major int
	minor int
}

func parseVersion(tag string) (version, error) {
	trimmed := strings.TrimPrefix(strings.TrimPrefix(tag, "release-"), "v")
	parts := strings.SplitN(trimmed, ".", 3)
	if len(parts) < 2 {
		return version{}, fmt.Errorf("version %q has no major.minor", tag)
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return version{}, fmt.Errorf("parsing major of %q: %w", tag, err)
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return version{}, fmt.Errorf("parsing minor of %q: %w", tag, err)
	}

	return version{
		major: major,
		minor: minor,
	}, nil
}

func mustParseVersion(tag string) version {
	v, err := parseVersion(tag)
	if err != nil {
		panic(fmt.Sprintf("KCP_VERSION: %v", err))
	}
	return v
}

func (v version) before(major, minor int) bool {
	if v.major != major {
		return v.major < major
	}
	return v.minor < minor
}

func (v version) after(major, minor int) bool {
	if v.major != major {
		return v.major > major
	}
	return v.minor > minor
}
