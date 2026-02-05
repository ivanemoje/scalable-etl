# Variables
DB_PATH = data/outputs/scalable.db
BRONZE_PATH = data/outputs/bronze_listens

up:
	docker compose up -d --build

down:
	docker compose down

volumes:
	docker compose down --volumes

zap: 
	@# Check if the iceberg container is running
	@docker ps | grep -q spark-iceberg-scalable || (echo "Error: Infrastructure is down. Run 'make up' first." && exit 1)
	
	@echo ">>> [1/3] Ingesting (DuckDB)..."
	python3 src/jobs/ingest_job.py
	
	@echo ">>> [2/3] Transforming (Spark)..."
	docker exec -it spark-iceberg-scalable spark-submit --master local[*] src/jobs/transform_job.py
	
	@echo ">>> [3/3] Daily Queries (DuckDB Query API)..."
	python3 src/jobs/daily_job.py

test:
	@echo ">>> Validating Raw Layer (Parquet Partitioning)..."
	@find $(BRONZE_PATH) -name "*.parquet" | head -n 5
	@echo "\n>>> Validating Gold Layer (DuckDB Metadata)..."
	@duckdb $(DB_PATH) "SELECT * FROM processed_files LIMIT 5;"

.PHONY: up down volumes zap test