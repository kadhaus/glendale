LINT_FILES=\
	google_indexing_api_client

flake:
	flake8 $(LINT_FILES)

mypy:
	mypy $(LINT_FILES)