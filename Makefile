COMPOSE_DIR := infrastructure/docker-compose
COMPOSE_BASE := docker-compose --env-file $(COMPOSE_DIR)/.env -f $(COMPOSE_DIR)/docker-compose.base.yml
COMPOSE_INFRA := $(COMPOSE_BASE) -f $(COMPOSE_DIR)/docker-compose.kafka.yml -f $(COMPOSE_DIR)/docker-compose.storage.yml -f $(COMPOSE_DIR)/docker-compose.flink.yml
COMPOSE_ALL := $(COMPOSE_INFRA) -f $(COMPOSE_DIR)/docker-compose.services.yml

.PHONY: help up down up-infra down-infra up-services down-services \
        up-kafka down-kafka up-storage down-storage up-flink down-flink \
        up-bridge down-bridge build-bridge logs-bridge register-schemas \
        build-flink submit-sql-pipeline submit-python-pipeline submit-all-flink \
        logs health ps \
        teardown teardown-destroy build-de-stock

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}'

# === Full Stack ===

up: build-de-stock ## Start everything (infra + services, without Flink)
	$(COMPOSE_ALL) up -d

down: ## Stop everything (preserves data)
	$(COMPOSE_ALL) down

ps: ## Show running containers
	$(COMPOSE_ALL) ps

logs: ## Tail logs for all services
	$(COMPOSE_ALL) logs -f --tail=50

# === Infrastructure ===
# Flink uses a Docker compose profile ("flink") so it doesn't start by default.
# Use `make up-flink` to start Flink separately (requires >= 4GB Docker memory).

up-infra: ## Start Kafka + Storage (Flink is opt-in via make up-flink)
	$(COMPOSE_INFRA) up -d

down-infra: ## Stop all infrastructure
	$(COMPOSE_INFRA) down

up-kafka: ## Start Kafka broker + Schema Registry + Kafka UI + topics
	$(COMPOSE_INFRA) up -d kafka-broker-1 schema-registry kafka-ui kafka-init

down-kafka: ## Stop Kafka
	$(COMPOSE_INFRA) stop kafka-ui schema-registry kafka-broker-1
	$(COMPOSE_INFRA) rm -f kafka-ui schema-registry kafka-init kafka-broker-1

up-storage: ## Start MinIO + Iceberg REST catalog
	$(COMPOSE_INFRA) up -d minio minio-init iceberg-rest

down-storage: ## Stop storage services
	$(COMPOSE_INFRA) stop iceberg-rest minio
	$(COMPOSE_INFRA) rm -f iceberg-rest minio-init minio

up-flink: build-flink ## Start Flink cluster (requires >= 4GB Docker memory)
	$(COMPOSE_INFRA) --profile flink up -d flink-jobmanager flink-taskmanager

down-flink: ## Stop Flink cluster
	$(COMPOSE_INFRA) --profile flink stop flink-taskmanager flink-jobmanager
	$(COMPOSE_INFRA) --profile flink rm -f flink-taskmanager flink-jobmanager

build-flink: ## Build Flink processor Docker image
	$(COMPOSE_INFRA) --profile flink build flink-jobmanager flink-taskmanager

submit-sql-pipeline: ## Submit Flink Job A (SQL trade aggregation + enrichment)
	docker exec flink-jobmanager python3 -m flink_processor.submit_sql_pipeline

submit-python-pipeline: ## Submit Flink Job B (price alerts + orderbook analytics)
	docker exec flink-jobmanager python3 -m flink_processor.submit_python_pipeline

submit-all-flink: submit-sql-pipeline submit-python-pipeline ## Submit all Flink jobs

# === Services ===

up-services: build-de-stock ## Start DE-Stock simulator
	$(COMPOSE_ALL) up -d de-stock

down-services: ## Stop DE-Stock simulator
	$(COMPOSE_ALL) stop de-stock
	$(COMPOSE_ALL) rm -f de-stock

build-de-stock: ## Build DE-Stock Docker image
	$(COMPOSE_ALL) build de-stock

build-bridge: ## Build Kafka Bridge Docker image
	$(COMPOSE_ALL) build kafka-bridge

up-bridge: build-bridge ## Start Kafka Bridge (requires infra + de-stock)
	$(COMPOSE_ALL) up -d kafka-bridge

down-bridge: ## Stop Kafka Bridge
	$(COMPOSE_ALL) stop kafka-bridge
	$(COMPOSE_ALL) rm -f kafka-bridge

register-schemas: ## Register Avro schemas with Schema Registry
	@bash infrastructure/scripts/register-schemas.sh

# === Health & Monitoring ===

health: ## Run health checks for all services
	@bash scripts/health-check.sh

# === Teardown ===

teardown: ## Stop all services (preserves data volumes)
	$(COMPOSE_ALL) down

teardown-destroy: ## Stop all and destroy data volumes
	$(COMPOSE_ALL) down -v --remove-orphans
	@echo "All containers stopped and volumes destroyed."

# === Logs (per service) ===

logs-kafka: ## Tail Kafka broker logs
	$(COMPOSE_INFRA) logs -f --tail=50 kafka-broker-1

logs-de-stock: ## Tail DE-Stock simulator logs
	$(COMPOSE_ALL) logs -f --tail=50 de-stock

logs-bridge: ## Tail Kafka Bridge logs
	$(COMPOSE_ALL) logs -f --tail=50 kafka-bridge

logs-flink: ## Tail Flink logs
	$(COMPOSE_INFRA) --profile flink logs -f --tail=50 flink-jobmanager flink-taskmanager
