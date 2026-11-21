# Shopping Sessions. `make help` lists the targets.
IMAGE    ?= shopping-sessions:latest
CPUS     ?= 8
MEM      ?= 24g
# The scorer's platform. PLATFORM=linux/arm64 runs natively on an Apple-silicon Mac.
PLATFORM ?= linux/amd64
PY       ?= .venv/bin/python
DEV      ?= data/dev
BIN      := pipeline/target/release/pipeline
GENERATE := tools/generator/target/release/generate
export IMAGE CPUS MEM PLATFORM

.PHONY: help build venv test fixtures refs dev check-dev sweep image image-fixtures image-dev

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

build: ## Build the pipeline and the dataset generator (native, release)
	cd pipeline && cargo build --release
	cd tools/generator && cargo build --release

venv: ## Create .venv with what the test scripts need
	uv venv -q .venv && uv pip install -q --python .venv/bin/python polars pyarrow

test: build ## Unit tests, then the 36 fixtures
	cd pipeline && cargo test --release
	$(PY) scripts/run_fixtures.py tests/fixtures -- $(BIN)

fixtures: build ## The 36 fixtures against the native binary
	$(PY) scripts/run_fixtures.py tests/fixtures -- $(BIN)

refs: ## The 36 fixtures against both Python references
	$(PY) scripts/run_fixtures.py tests/fixtures -- $(PY) reference/a/reference.py
	$(PY) scripts/run_fixtures.py tests/fixtures -- $(PY) reference/b/reference.py

dev: build ## Generate the 1M-event dev set
	$(GENERATE) --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

check-dev: build ## Run the native binary on the dev set and compare with the published answer
	rm -rf out && $(BIN) $(DEV) out && $(PY) scripts/compare.py out tests/dev-answer

sweep: build ## Pipeline against both references on many seeds: make sweep SEEDS=1-50
	$(PY) scripts/sweep.py --seeds $(or $(SEEDS),1-20)

image: ## Build the Docker image the scorer will run
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

image-fixtures: image ## The 36 fixtures against the image, with the scorer's container limits
	$(PY) scripts/run_fixtures.py tests/fixtures -- scripts/docker_run.sh

image-dev: image ## The dev set against the image, timed, then compared with the answer
	rm -rf out && time scripts/docker_run.sh $(DEV) out && $(PY) scripts/compare.py out tests/dev-answer
