# Copyright 2026 The kcp Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

export CGO_ENABLED ?= 0
GO ?= go

default: lint lint-readme verify test

.PHONY: lint
lint: install-golangci-lint
	$(GOLANGCI_LINT) run ./...

.PHONY: lint-fix
lint-fix: override GOLANGCI_LINT_FLAGS := $(GOLANGCI_LINT_FLAGS) --fix
lint-fix: lint

.PHONY: lint-readme
lint-readme:
	rm -f readme_test.go
	$(GO) tool mdextract -tags ci -output readme_test.go README.md
	@# only checking that the code in the readme is valid
	go vet readme_test.go
	rm -f readme_test.go

.PHONY: verify
verify:
	./hack/verify-boilerplate.sh

.PHONY: test
test:
	$(GO) test ./...

export UGET_DIRECTORY = hack/tools
export UGET_CHECKSUMS = hack/tools.checksums
export UGET_VERSIONED_BINARIES = true

BOILERPLATE_VERSION ?= 0.3.0
GOLANGCI_LINT_VERSION ?= 2.15.0

.PHONY: install-boilerplate
install-boilerplate:
	@hack/uget.sh https://github.com/kubermatic-labs/boilerplate/releases/download/v{VERSION}/boilerplate_{VERSION}_{GOOS}_{GOARCH}.tar.gz boilerplate $(BOILERPLATE_VERSION)

GOLANGCI_LINT = $(UGET_DIRECTORY)/golangci-lint-$(GOLANGCI_LINT_VERSION)

.PHONY: install-golangci-lint
install-golangci-lint:
	@hack/uget.sh https://github.com/golangci/golangci-lint/releases/download/v{VERSION}/golangci-lint-{VERSION}-{GOOS}-{GOARCH}.tar.gz golangci-lint $(GOLANGCI_LINT_VERSION)

.PHONY: update-tools
update-tools: UGET_UPDATE=true
update-tools: clean-tools install-boilerplate install-golangci-lint

.PHONY: clean-tools
clean-tools:
	rm -rf $(UGET_DIRECTORY)
	@echo "Cleaned $(UGET_DIRECTORY)."
