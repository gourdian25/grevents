# File: Makefile

# ==============================================================================
# This Makefile is designed for a Go *library*
# - No build targets
# - Strong linting & testing
# - CI-safe targets
# - Explicit release guardrails
# ==============================================================================

.DEFAULT_GOAL := help

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
GO              := go
GOLANGCI_LINT   := golangci-lint
COVERAGE_DIR    := test_coverage
COVERAGE_MIN    := 95

# VERSION must be explicitly provided for release/tag
VERSION ?=

# ------------------------------------------------------------------------------
# Guardrails
# ------------------------------------------------------------------------------
.PHONY: guard-version
guard-version: ## Ensure VERSION is provided (required for release/tag)
	@if [ -z "$(VERSION)" ]; then \
		echo "❌ VERSION is required (example: make release VERSION=v0.1.0)"; \
		exit 1; \
	fi

# ------------------------------------------------------------------------------
# Help
# ------------------------------------------------------------------------------
.PHONY: help
help: ## Show available Makefile targets
	@echo ""
	@echo "📦 grevents — Go Library Makefile"
	@echo "────────────────────────────────────────"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"} \
	/^[a-zA-Z0-9_-]+:.*##/ { \
		printf "  \033[32m%-24s\033[0m %s\n", $$1, $$2 \
	}' $(MAKEFILE_LIST)
	@echo ""

# ------------------------------------------------------------------------------
# Formatting & Hygiene
# ------------------------------------------------------------------------------
.PHONY: fmt
fmt: ## Format Go code
	$(GO) fmt ./...

.PHONY: tidy
tidy: ## Run go mod tidy
	$(GO) mod tidy

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

# ------------------------------------------------------------------------------
# Linting
# ------------------------------------------------------------------------------
.PHONY: lint
lint: ## Run golangci-lint
	$(GOLANGCI_LINT) run ./...

.PHONY: lint-fix
lint-fix: ## Run golangci-lint with auto-fix
	$(GOLANGCI_LINT) run --fix ./...

# ------------------------------------------------------------------------------
# Testing
# ------------------------------------------------------------------------------
.PHONY: test
test: ## Run all unit tests
	$(GO) test -cover ./...

.PHONY: race
race: ## Run tests with race detector (mandatory before any commit touching delivery code)
	env CGO_ENABLED=1 $(GO) test -race -v ./...

.PHONY: bench
bench: ## Run benchmarks
	$(GO) test -bench=. -benchmem -run=^$$ ./...

# ------------------------------------------------------------------------------
# Coverage
# ------------------------------------------------------------------------------
$(COVERAGE_DIR):
	mkdir -p $(COVERAGE_DIR)

.PHONY: coverage
coverage: $(COVERAGE_DIR) ## Generate HTML coverage report
	$(GO) test -coverprofile=$(COVERAGE_DIR)/coverage.out ./...
	$(GO) tool cover -html=$(COVERAGE_DIR)/coverage.out \
		-o $(COVERAGE_DIR)/coverage.html
	@echo "✔ Coverage report generated: $(COVERAGE_DIR)/coverage.html"

.PHONY: coverage-summary
coverage-summary: $(COVERAGE_DIR) ## Print per-function coverage summary
	$(GO) test -coverprofile=$(COVERAGE_DIR)/coverage.out ./...
	$(GO) tool cover -func=$(COVERAGE_DIR)/coverage.out

.PHONY: coverage-check
# Only the root package is checked: example is a runnable demo, not
# library code under test — matching grcache's own coverage-check
# convention exactly. The former conformance/ subpackage was folded into
# the root package's own contract_bus_test.go, so there is nothing else
# to exclude.
coverage-check: $(COVERAGE_DIR) ## Fail if root package coverage drops below COVERAGE_MIN
	@out=$$($(GO) test -cover . 2>&1); \
	echo "$$out"; \
	pct=$$(echo "$$out" | grep -o '[0-9.]*%' | tr -d '%'); \
	if [ -z "$$pct" ]; then echo "❌ no coverage output"; exit 1; fi; \
	awk -v p="$$pct" -v m="$(COVERAGE_MIN)" 'BEGIN { exit !(p >= m) }' \
		|| { echo "❌ Coverage $$pct%% is below minimum $(COVERAGE_MIN)%%"; exit 1; }

# ------------------------------------------------------------------------------
# Cleanup
# ------------------------------------------------------------------------------
.PHONY: clean
clean: ## Clean generated files and test cache
	rm -rf $(COVERAGE_DIR)
	$(GO) clean -testcache

# ------------------------------------------------------------------------------
# CI Targets (DO NOT use locally)
# ------------------------------------------------------------------------------
.PHONY: ci-test
ci-test: ## [CI] Run tests
	$(GO) test ./...

.PHONY: ci-lint
ci-lint: ## [CI] Run linter
	$(GOLANGCI_LINT) run ./...

.PHONY: ci
ci: ci-lint ci-test ## [CI] Run full CI pipeline

# ------------------------------------------------------------------------------
# Release
# ------------------------------------------------------------------------------
.PHONY: tag
tag: guard-version ## Create and push git tag
	git tag $(VERSION)
	git push origin $(VERSION)

.PHONY: release
release: guard-version tag ## Create release using GoReleaser
	goreleaser release --clean

.PHONY: goreleaser-check
goreleaser-check: ## Dry run: validate config + snapshot release (no tag/push)
	goreleaser check
	goreleaser release --snapshot --clean
