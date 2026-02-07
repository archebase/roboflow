.PHONY: all build build-release test test-all coverage coverage-rust clippy fmt lint clean check-license dev-up dev-down dev-logs dev-ps dev-restart dev-clean help

# Default target
all: build

# ============================================================================
# Build targets
# ============================================================================

build: ## Build Rust library (debug)
	@echo "Building roboflow (debug)..."
	cargo build
	@echo "✓ Build complete"

build-release: ## Build Rust library (release)
	@echo "Building roboflow (release)..."
	cargo build --release
	@echo "✓ Build complete (release)"

# ============================================================================
# Testing
# ============================================================================

test: ## Run Rust tests
	@echo "Running Rust tests..."
	cargo test
	@echo "✓ Rust tests passed (run 'make test-all' for dataset features)"

test-all: ## Run all tests including dataset features (requires HDF5)
	@echo "Running all tests with all features..."
	@echo "  (features: dataset-all)"
	cargo test --features dataset-all
	@echo "✓ All tests passed"

# ============================================================================
# Coverage
# ============================================================================

coverage: coverage-rust ## Run coverage report
	@echo ""
	@echo "✓ Coverage report generated"
	@echo "  Rust:   target/llvm-cov/html/index.html"

coverage-rust: ## Run Rust tests with coverage (requires cargo-llvm-cov)
	@echo "Running Rust tests with coverage..."
	@echo "(Install: cargo install cargo-llvm-cov)"
	cargo llvm-cov --workspace --html --output-dir target/llvm-cov/html
	cargo llvm-cov --workspace --lcov --output-path lcov.info
	@echo ""
	@echo "✓ Rust coverage report: target/llvm-cov/html/index.html (add --features dataset-all for dataset coverage)"

# ============================================================================
# Code quality
# ============================================================================

fmt: ## Format Rust code
	@echo "Formatting Rust code..."
	cargo fmt
	@echo "✓ Rust code formatted"

lint: ## Lint Rust code with clippy
	@echo "Linting Rust code..."
	cargo clippy --all-targets --all-features -- -D warnings
	@echo "✓ Rust linting passed"

lint-all: ## Lint with all features including HDF5 (requires compatible HDF5)
	@echo "Linting with all features..."
	cargo clippy --all-targets --all-features -- -D warnings
	@echo "✓ All linting passed"

fix: ## Auto-fix linting issues
	@echo "Auto-fixing issues..."
	cargo fmt
	@echo "✓ Issues fixed"

check: fmt lint ## Run format check and lint

check-license: ## Check REUSE license compliance
	@echo "Checking REUSE license compliance..."
	@if command -v reuse >/dev/null 2>&1; then \
		reuse lint; \
	else \
		echo "⚠ reuse tool not found. Install with: pip install reuse"; \
		exit 1; \
	fi

# ============================================================================
# Development (docker-compose)
# ============================================================================

dev-up: ## Start development services with docker-compose
	@echo "Starting development services..."
	docker compose up -d
	@echo "✓ Services started"
	@echo "  Use 'make dev-logs' to view logs"
	@echo "  Use 'make dev-ps' to view service status"

dev-down: ## Stop development services
	@echo "Stopping development services..."
	docker compose down
	@echo "✓ Services stopped"

dev-logs: ## View logs from development services
	docker compose logs -f

dev-ps: ## Show status of development services
	docker compose ps

dev-restart: ## Restart development services
	@echo "Restarting development services..."
	docker compose restart
	@echo "✓ Services restarted"

dev-clean: ## Stop and remove all development containers, volumes, networks, and local data
	@echo "Cleaning up development environment..."
	docker compose down -v
	rm -rf output/ lerobot_config*.toml
	@echo "✓ Development environment cleaned"
	@echo "  Containers, volumes, networks, and local data removed"

# ============================================================================
# Utilities
# ============================================================================

clean: ## Clean build artifacts
	@echo "Cleaning..."
	cargo clean
	rm -rf target/
	rm -f lcov.info
	@echo "✓ Cleaned"

# ============================================================================
# Publishing
# ============================================================================

publish: ## Publish to crates.io
	@echo "Publishing to crates.io..."
	cargo publish
	@echo "✓ Published to crates.io"

help: ## Show this help message
	@echo "Roboflow - Distributed data transformation pipeline"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
