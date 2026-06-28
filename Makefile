# VoltSense — Phase 0 Makefile
# Local-first dev. No cloud targets yet (those arrive in Phase 3).
#
# If your WSL distro doesn't have Docker integration enabled, the `docker`
# command may resolve to the Windows client (docker.exe via Docker Desktop).
# Override with: make up DOCKER="docker.exe"
DOCKER  ?= docker
COMPOSE ?= $(DOCKER) compose

.DEFAULT_GOAL := help

.PHONY: help up down logs ps restart health topics clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

up: ## Start the local stack (Kafka + Schema Registry + Qdrant) in the background
	$(COMPOSE) up -d

down: ## Stop and remove the stack containers (volumes kept)
	$(COMPOSE) down

logs: ## Tail logs from all services (Ctrl-C to stop)
	$(COMPOSE) logs -f

ps: ## Show service status
	$(COMPOSE) ps

restart: ## Restart the whole stack
	$(COMPOSE) restart

health: ## Prove the stack works: list Kafka topics + check Qdrant
	@echo "== Kafka topics (from inside the container) =="
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:9092 --list || true
	@echo "== Schema Registry subjects =="
	@curl -fsS http://localhost:8081/subjects && echo || echo "schema-registry not reachable"
	@echo "== Qdrant health (host -> localhost:6333) =="
	@curl -fsS http://localhost:6333/healthz && echo || echo "qdrant not reachable"

topics: ## List Kafka topics
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:9092 --list

clean: ## Stop the stack AND delete volumes (wipes Kafka + Qdrant data)
	$(COMPOSE) down -v
