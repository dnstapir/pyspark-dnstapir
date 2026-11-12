all:

test:
	uv run ./tests/run_tests.sh --ruff --ruff-format

lint:
	uv run ruff check .

reformat:
	uv run ruff check --select I --fix .
	uv run ruff format .

clean:

realclean: clean
	rm -fr .venv
