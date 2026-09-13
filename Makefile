SEED ?= 42

.PHONY: test replay health-llm lint up down logs

test:
	uv run pytest -q

replay:
	uv run python -m sim.loop --seed $(SEED) $(ARGS)

health-llm:
	uv run uvicorn llm.service:app --port 8000

lint:
	uvx ruff check .

up:
	docker compose -f infra/docker-compose.yml up --build

down:
	docker compose -f infra/docker-compose.yml down

logs:
	docker compose -f infra/docker-compose.yml logs -f --tail=100
