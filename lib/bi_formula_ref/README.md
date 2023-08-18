# BI formula reference

Package to generate BI docs source

## Development

### Makefile commands

- `make init-venv` - Initialize dependencies for doc generation
- `make clean-venv` - Clean dependencies for doc generation
- `make generate-example-data` - Generate example data
- `DOCS_SOURCE_PATH={docs-source repo path} make generate-docs-source` - Generate docs,
alternatively you can leave DOCS_SOURCE_PATH empty and docs will be generated locally to `docs-source` folder
