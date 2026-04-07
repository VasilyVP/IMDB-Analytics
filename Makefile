
# Start all services in detached mode, create Neo4j instance.
install:
	@docker compose up -d

# Run the setup script to populate the database with initial data
seed:
	@uv run python scripts/imdb_seed.py
	@uv run python scripts/neo4j_seed.py

# Seed Neo4j with only the first 100 titles and their related persons/relationships (for dev/testing)
seed-sample:
	@uv run python scripts/imdb_seed.py
	@uv run python scripts/neo4j_seed.py --limit 1000

# Resume previously stopped containers (no config reload)
start:
	@docker compose start

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

# Tail logs from all services
logs:
	@docker compose logs -f

# Show running container status
status:
	@docker compose ps
