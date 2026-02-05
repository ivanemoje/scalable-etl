# Variables
DB_PATH = data/outputs/scalable.db
BRONZE_PATH = data/outputs/bronze_listens

up:
	docker compose up -d --build

down:
	docker compose down

volumes:
	docker compose down --volumes

# Original zap - runs ingestion locally (conflicts with Docker ingestion)
zap-local: 
	@# Check if the iceberg container is running
	@docker ps | grep -q spark-iceberg-scalable || (echo "Error: Infrastructure is down. Run 'make up' first." && exit 1)
	@echo ">>> [1/3] Ingesting (DuckDB)..."
	python3 src/jobs/ingest_job.py &
	@sleep 3
	@echo ">>> [2/3] Transforming (Spark)..."
	docker exec -it spark-iceberg-scalable spark-submit --master local[*] /home/iceberg/src/jobs/transform_job.py
	@echo ">>> [3/3] Daily Queries (DuckDB Query API)..."
	python3 src/jobs/daily_job.py
	@pkill -f ingest_job.py || true

# New zap - uses Docker ingestion (no conflicts)
zap:
	@# Check if containers are running
	@docker ps | grep -q ingest-watcher || (echo "Error: Ingestion container not running. Run 'make up' first." && exit 1)
	@docker ps | grep -q spark-iceberg-scalable || (echo "Error: Spark container not running. Run 'make up' first." && exit 1)
	@echo ">>> [1/3] Ingestion is running in Docker (ingest-watcher container)..."
	@echo "    Files in data/inputs/ are automatically processed."
	@echo "    To watch logs: docker compose logs -f ingest"
	@sleep 2
	@echo ""
	@echo ">>> [2/3] Transforming (Spark)..."
	docker exec spark-iceberg-scalable spark-submit --master local[*] /home/iceberg/src/jobs/transform_job.py
	@echo ""
	@echo ">>> [3/3] Daily Queries (DuckDB Query API)..."
	python3 src/jobs/daily_job.py

# Watch ingestion logs
logs-ingest:
	@echo ">>> Watching ingestion logs (Ctrl+C to stop)..."
	docker compose logs -f ingest

# Watch all logs
logs:
	docker compose logs -f

# Stop the Docker ingestion and run locally (for debugging)
ingest-local-only:
	@echo ">>> Stopping Docker ingestion..."
	docker compose stop ingest
	@echo ">>> Running local ingestion (Ctrl+C to stop)..."
	python3 src/jobs/ingest_job.py

# Restart Docker ingestion
ingest-docker-restart:
	docker compose restart ingest
	@echo ">>> Docker ingestion restarted"
	@sleep 2
	docker compose logs --tail=20 ingest

test:
	@echo ">>> Validating Raw Layer (Parquet Partitioning)..."
	@find $(BRONZE_PATH) -name "*.parquet" 2>/dev/null | head -n 5 || echo "No parquet files found yet"
	@echo ""
	@echo ">>> Validating Gold Layer (DuckDB Metadata)..."
	@duckdb $(DB_PATH) "SELECT * FROM processed_files LIMIT 5;" 2>/dev/null || echo "No processed files yet"

# Show pipeline status
status:
	@echo ">>> Container Status:"
	@docker compose ps
	@echo ""
	@echo ">>> Ingestion Status:"
	@docker compose logs --tail=5 ingest 2>/dev/null || echo "Ingestion container not running"
	@echo ""
	@echo ">>> Files in Bronze Layer:"
	@find $(BRONZE_PATH) -name "*.parquet" 2>/dev/null | wc -l | xargs echo "Parquet files:"

# Clean all data
clean:
	rm -rf data/outputs/*
	rm -rf warehouse/*
	@echo ">>> Cleaned all generated data"

.PHONY: up down volumes zap zap-local logs logs-ingest ingest-local-only ingest-docker-restart test status clean
