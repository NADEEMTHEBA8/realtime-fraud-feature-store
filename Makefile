.PHONY: help up down logs ps clean install fmt lint test gen-tx

help:
	@echo "Available targets:"
	@echo "  up        - Start all docker services"
	@echo "  down      - Stop all docker services"
	@echo "  ps        - Show running services"
	@echo "  logs      - Tail logs (use SVC=kafka to filter)"
	@echo "  clean     - Stop services and remove volumes (destroys data)"
	@echo "  install   - Install python dependencies"
	@echo "  fmt       - Format python code with ruff"
	@echo "  lint      - Lint python code with ruff"
	@echo "  test      - Run all unit tests"

up:
	docker compose up -d
	@echo "Services starting. Check status with: make ps"
	@echo "Kafka UI:  http://localhost:8080"
	@echo "MinIO UI:  http://localhost:9001"

down:
	docker compose down

ps:
	docker compose ps

logs:
ifdef SVC
	docker compose logs -f $(SVC)
else
	docker compose logs -f
endif

clean:
	docker compose down -v
	@echo "All containers and volumes removed."

install:
	pip install -e ".[dev]"

fmt:
	ruff format .

lint:
	ruff check .

test:
	pytest -v