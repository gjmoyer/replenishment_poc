SEED ?= 42

.PHONY: install test coverage lint fmt typecheck replay health-llm ci release image pull up-image up down logs

install:            ## Install dev deps and git hooks
	uv sync --dev
	uv run pre-commit install

test:
	uv run pytest -q

coverage:
	uv run pytest --cov --cov-report=term-missing

lint:
	uv run ruff check .
	uv run ruff format --check .

fmt:
	uv run ruff check --fix .
	uv run ruff format .

typecheck:
	uv run mypy .

replay:
	uv run python -m sim.loop --seed $(SEED) $(ARGS)

health-llm:
	uv run uvicorn llm.service:app --port 8000

ci: lint typecheck test replay   ## Exactly what GitHub Actions runs

release:
	uv build

image:
	docker build -f infra/Dockerfile -t replenishment-poc:dev .

pull:               ## Pull the published GHCR image (POC_TAG=0.1.1 to pin)
	docker compose -f infra/docker-compose.yml pull

up-image: pull      ## Run the stack from the published image (no local build)
	docker compose -f infra/docker-compose.yml up

up:
	docker compose -f infra/docker-compose.yml up --build

down:
	docker compose -f infra/docker-compose.yml down

logs:
	docker compose -f infra/docker-compose.yml logs -f --tail=100
