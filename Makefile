.PHONY: all build build-release test test-rust test-python test-all coverage coverage-rust coverage-python clippy fmt lint clean publish publish-pypi publish-crates check-license help

# Default target
all: build

# ============================================================================
# Build targets
# ============================================================================

build: ## Build Rust library (debug)
	@echo "Building robocodec (debug)..."
	cargo build
	@echo "✓ Build complete"

build-release: ## Build Rust library (release)
	@echo "Building robocodec (release)..."
	cargo build --release
	@echo "✓ Build complete (release)"

build-python: ## Build Python wheel (debug)
	@echo "Building Python wheel..."
	maturin build
	@echo "✓ Python wheel built (see target/wheels/)"

build-python-release: ## Build Python wheel (release)
	@echo "Building Python wheel (release)..."
	maturin build --release --strip
	@echo "✓ Python wheel built (release, see target/wheels/)"

build-python-dev: ## Install Python package in dev mode (requires virtualenv)
	@echo "Installing Python package in dev mode..."
	maturin develop --features python
	@echo "✓ Python package installed"

# ============================================================================
# Testing
# ============================================================================

test: test-rust test-python ## Run all tests
	@echo "✓ All tests passed"

test-rust: ## Run Rust tests
	@echo "Running Rust tests..."
	cargo test
	@echo "✓ Rust tests passed (run 'make test-all' for Kps features)"

test-all: ## Run all tests including Kps features (requires HDF5)
	@echo "Running all tests with all features..."
	@echo "  (features: kps-all)"
	cargo test --features kps-all
	@echo "✓ All tests passed"

test-python: ## Run Python tests (builds extension first)
	@echo "Building Python extension..."
	maturin develop --features python
	@echo "Running Python tests..."
	pytest python/ -v
	@echo "✓ Python tests passed"

# ============================================================================
# Coverage
# ============================================================================

coverage: coverage-rust coverage-python ## Run all coverage reports
	@echo ""
	@echo "✓ Coverage reports generated"
	@echo "  Rust:   target/llvm-cov/html/index.html"
	@echo "  Python: coverage-html/index.html"

coverage-rust: ## Run Rust tests with coverage (requires cargo-llvm-cov)
	@echo "Running Rust tests with coverage..."
	@echo "(Install: cargo install cargo-llvm-cov)"
	cargo llvm-cov --workspace --html --output-dir target/llvm-cov/html
	cargo llvm-cov --workspace --lcov --output-path lcov.info
	@echo ""
	@echo "✓ Rust coverage report: target/llvm-cov/html/index.html (add --features kps-all for Kps coverage)"

coverage-python: ## Run Python tests with coverage
	@echo "Running Python tests with coverage..."
	pytest python/ --cov=roboflow --cov-report=term-missing --cov-report=html:coverage-html --cov-report=xml:coverage.xml
	@echo ""
	@echo "✓ Python coverage report: coverage-html/index.html"

# ============================================================================
# Code quality
# ============================================================================

fmt: fmt-rust fmt-python ## Format all code
	@echo "✓ All code formatted"

fmt-rust: ## Format Rust code
	@echo "Formatting Rust code..."
	cargo fmt
	@echo "✓ Rust code formatted"

fmt-python: ## Format Python code with ruff
	@echo "Formatting Python code with ruff..."
	ruff format python/
	@echo "✓ Python code formatted"

lint: lint-rust lint-python ## Lint all code
	@echo "✓ All linting passed"

lint-rust: ## Lint Rust code with clippy
	@echo "Linting Rust code..."
	cargo clippy --all-targets --all-features -- -D warnings
	@echo "✓ Rust linting passed"

lint-python: ## Lint Python code with ruff
	@echo "Linting Python code with ruff..."
	ruff check python/
	@echo "✓ Python linting passed"

lint-all: ## Lint with all features including HDF5 (requires compatible HDF5)
	@echo "Linting with all features..."
	cargo clippy --all-targets --all-features -- -D warnings
	ruff check python/
	@echo "✓ All linting passed"

fix: ## Auto-fix linting issues
	@echo "Auto-fixing issues..."
	cargo fmt
	ruff format python/
	ruff check python/ --fix
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
# Utilities
# ============================================================================

clean: ## Clean build artifacts
	@echo "Cleaning..."
	cargo clean
	rm -rf target/
	rm -rf **/__pycache__/
	rm -rf **/.pytest_cache/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf coverage-html/
	rm -f coverage.xml lcov.info
	@echo "✓ Cleaned"

# ============================================================================
# Publishing
# ============================================================================

publish: publish-pypi publish-crates ## Publish to PyPI and crates.io

publish-pypi: ## Publish to PyPI (requires twine)
	@echo "Publishing to PyPI..."
	maturin build --release --strip --out dist
	twine upload dist/robocodec*.whl
	@echo "✓ Published to PyPI"

publish-crates: ## Publish to crates.io
	@echo "Publishing to crates.io..."
	cargo publish
	@echo "✓ Published to crates.io"

help: ## Show this help message
	@echo "Robocodec - Robotics Message Codec"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
