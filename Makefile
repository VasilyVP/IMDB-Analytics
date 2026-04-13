# Start the full setup: install services and seed data.
setup:
	@echo "Starting installation..."
	@$(MAKE) install
	@echo "Seeding data..."
	@$(MAKE) seed
	@echo "Done."

# Start all services in detached mode, create Neo4j instance.
install:
	@docker compose up -d --wait

# Run the setup script to populate the database with initial data
seed:
	@uv run python scripts/imdb_seed.py
	@uv run python scripts/neo4j_seed.py

# Seed Neo4j with only the first 100 titles and their related persons/relationships (for dev/testing)
seed-sample:
	@uv run python scripts/imdb_seed.py
	@uv run python scripts/neo4j_seed.py --limit 1000

# Run back-end unit tests
test:
	@uv run --directory back-end python -m unittest discover -s tests -p "test_*.py"

# Resume previously stopped containers (no config reload)
start:
	@docker compose start --wait

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
