# Start the full setup: install services and seed data.
setup:
	@echo "Starting installation..."
	@$(MAKE) install
	@echo "Seeding data..."
	@$(MAKE) seed
	@echo "Done."

# Start default infrastructure services (Neo4j, ChromaDB) in detached mode and wait for health checks.
# To install with vLLM, use `make install:llm` instead to include it in the startup process.
install:
	@docker compose up -d --wait

# Start infrastructure including optional vLLM service.
install\:llm:
	@docker compose --profile llm up -d --wait

# Run the setup script to populate the database with initial data
seed:
	@uv run python scripts/imdb_seed.py
	@uv run python scripts/neo4j_seed.py
	@uv run python scripts/chroma_seed/main.py

# Seed Neo4j with only the first 100 titles and their related persons/relationships (for dev/testing)
seed-sample:
	@uv run python scripts/imdb_seed.py
	@uv run python scripts/neo4j_seed.py --limit 1000
	@uv run python scripts/chroma_seed/main.py --limit 1000

# Run back-end and scripts unit tests
test:
	@echo "Running back-end unit tests..."
	@uv run --directory back-end python -m unittest discover -s tests -p "test_*.py"
	@echo "Running scripts unit tests..."
	@uv run python -m unittest discover -s scripts/tests -p "test_*.py"

# Run only scripts integration tests
test\:integrational:
	@echo "Running scripts integration tests..."
	@uv run python -m unittest discover -s scripts/tests/integrational -p "test_*.py"

# Resume previously stopped default containers (Neo4j, ChromaDB) and wait for health checks.
# To include vLLM in the resumed services, use `make start:llm` instead.
start:
	@docker compose start --wait

# Resume containers including optional vLLM service.
start\:llm:
	@docker compose --profile llm up -d --wait

# Pause containers without removing them (data and state preserved)
stop:
	@docker compose stop

# Remove containers and networks (volumes kept); use after docker-compose.yml changes
teardown:
	@docker compose down

# Remove containers, networks, all volumes (wipes all data), clean DuckDB and Parquet files; use to reset everything
reset:
	@docker compose down -v
	@rm -f back-end/data/imdb.duckdb
	@rm -f back-end/data/*.parquet
	@rm -f back-end/data/sources/*.csv

# Start the Neo4J, fastapi development server and vite dev
dev: export PYTHONUTF8 = 1
dev:
	@echo "Starting development servers..."
	@$(MAKE) start
	@concurrently -n "FastAPI,Vite" -c "green,yellow" \
		"uv run --directory back-end fastapi dev app/main.py" \
		"cd front-end && bun run dev"

prod:
	@echo "Starting production servers..."
	fastapi run --directory back-end app/main.py

# Tail logs from all services
logs:
	@docker compose logs -f

# Show running container status
status:
	@docker compose ps
