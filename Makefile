build:
	@docker compose build

start:
	@docker compose up

background:
	@docker compose up -d

down:
	@docker compose down

destroy:
	@docker compose down -v

ruff:
	@docker compose run --rm --no-deps simulator ruff format .
	@docker compose run --rm --no-deps simulator ruff check --fix .
