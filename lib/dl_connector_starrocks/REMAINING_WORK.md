# StarRocks Connector - Known Issues & Future Work

**Last Updated**: 2026-03-09

---

## Known Issues

### 1. Infrastructure Test Failures (24 errors)

API tests fail due to United Storage (PostgreSQL) not being accessible during test initialization. This is a test environment issue, not a connector bug.

### 2. `test_closing_sql_sessions` (1 failure)

aiomysql `ResultProxy` attempts cursor cleanup after event loop closure. This is an upstream driver issue in aiomysql.

### 3. Dependency Lint Issues (pre-existing)

- `dl_app_tools` is imported as a transitive dependency (DEP003)
- `dl-configs` is declared as a dependency but unused (DEP002)

---

## Future Work

### SSL/TLS Support

Not implemented in MVP. Requires design decisions around certificate handling.

### Window Functions

No `functions_window.py` — missing ROW_NUMBER, RANK, LAG, LEAD, etc.

### `formula_ref` Plugin

Other connectors register a `dl_formula_ref.plugins` entry point for formula documentation generation. StarRocks doesn't have this yet.

### Extended Join Support

Currently supports INNER, LEFT, RIGHT joins. StarRocks also supports FULL OUTER and CROSS joins.
