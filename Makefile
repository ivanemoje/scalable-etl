# Variables
DB_PATH = data/outputs/scalable.db
BRONZE_PATH = data/outputs/bronze_listens
DOCKER_COMPOSE := $(shell command -v docker-compose 2> /dev/null || echo "docker compose")
COMPOSE_RUN := $(DOCKER_COMPOSE)

# --- Infrastructure ---

up:
	$(COMPOSE_RUN) up -d --build

down:
	$(COMPOSE_RUN) down

# Soft Reset: Restarts and rebuilds code/containers but KEEPS volumes/data
zap:
	$(COMPOSE_RUN) down
	$(COMPOSE_RUN) up -d --build

# Hard Reset: Wipes everything including volumes and local data folders
.zap:
	$(COMPOSE_RUN) down --volumes
	$(MAKE) clean
	$(COMPOSE_RUN) up -d --build

rebuild: clean
	$(COMPOSE_RUN) build --no-cache
	$(COMPOSE_RUN) up -d

# --- Pipeline Execution ---

# Run the Spark jobs
jobs:
	@docker ps | grep -q ingest-watcher || (echo "Error: ingest-watcher not running. Run 'make up'." && exit 1)
	@docker ps | grep -q spark-iceberg-scalable || (echo "Error: spark-iceberg not running. Run 'make up'." && exit 1)
	@echo ">>> [1/3] Checking Ingestion Status..."
	@if [ ! -d "$(BRONZE_PATH)" ] || [ $$(find $(BRONZE_PATH) -name "*.parquet" | wc -l) -eq 0 ]; then \
		echo "Waiting for ingest-watcher to process files..."; \
		sleep 5; \
	fi
	@echo ">>> [2/3] Transforming (Bronze -> Silver)..."
	docker exec spark-iceberg-scalable spark-submit --master local[*] /home/iceberg/src/jobs/transform_job.py
	@echo ">>> [3/3] Running Daily Aggregations (Silver -> Gold)..."
	docker exec spark-iceberg-scalable spark-submit --master local[*] /home/iceberg/src/jobs/daily_job.py

# --- Utilities ---

clean:
	rm -rf data/outputs/*
	rm -rf warehouse/*
	@echo ">>> Cleaned all local generated data"

.PHONY: up down zap .zap jobs rebuild clean