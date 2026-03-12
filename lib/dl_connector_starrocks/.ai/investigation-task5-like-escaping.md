# Task 5: CONTAINS/LIKE Parameter Escaping — Investigation Notes

## Status: In Progress (root cause not yet confirmed)

## What we know so far

### The Error
SQL: `SELECT %(param_1)s LIKE %(param_2)s ESCAPE '\\' AS anon_1` with params `{'param_1': 'Lorem ipsum', 'param_2': '%ips%'}` fails during execution.

Note: The error shows `pyformat` style (`%(param_1)s`), but actual dialect uses `format` style (`%s`).

### Architecture
- Formula tests use `bi_starrocks://` URL → creates SA engine with `BiStarRocksDialect`
- `BiStarRocksDialect` inherits from `DLMYSQLDialect` → inherits from `MySQLDialect_pymysql`
- All use `format` paramstyle (`%s`), positional=True
- MySQL connector has identical setup and presumably passes

### Key Files
- `dl_connector_starrocks/core/sa_dialect.py` — BiStarRocksDialect (name="bi_starrocks")
- `dl_connector_starrocks/core/async_adapters_starrocks.py:95` — uses `DLMYSQLDialect(paramstyle="pyformat")` for async adapter
- `dl_connector_mysql/core/utils.py` — `compile_mysql_query()` does manual `%` string formatting
- `dl_connector_starrocks/formula/connector.py:18` — `sa_dialect = SAMySQLDialect()` (for formula translation only)
- `dl_connector_starrocks/db_testing/engine_wrapper.py` — BiStarRocksEngineWrapper (URL_PREFIX="bi_starrocks")

### Formula test execution path
1. Formula evaluator builds SA expression: `literal('Lorem ipsum').like(literal('%ips%'), escape='\\')`
2. Wraps in `sa.select([expr])`
3. Calls `db.execute(query)` → `engine.execute(query)`
4. Engine uses BiStarRocksDialect to compile → `SELECT %s LIKE %s ESCAPE '\\' AS anon_1`
5. PyMySQL receives `format`-style query and positional params → does `query % escaped_args`

### Things verified
- Both MySQL and StarRocks formula connectors use `sa_dialect = SAMySQLDialect()`
- Both dialects have `paramstyle="format"`, `positional=True`
- Python `%` formatting with positional tuple works fine with `%ips%` in substituted values (single-pass, no re-interpretation)
- The `compile_mysql_query` function (in async adapter path) does manual `%` substitution but is NOT used in formula tests

### What needs further investigation
1. **Is the error actually from the formula DB tests or from a different test path?**
   - The error shows `pyformat` style which would come from the async adapter path (line 95 of async_adapters_starrocks.py: `paramstyle="pyformat"`)
   - In pyformat mode, `compile_mysql_query` builds `%(name)s` markers and the value `%ips%` in the params dict should still be fine for pymysql
   - But maybe there's an issue with double-escaping or the `escape_percent` logic in `compile_mysql_query`

2. **The async adapter path** (`compile_mysql_query` with `escape_percent=True`):
   - Line 46: `new_query = new_query.replace("%%", "%%%%")`
   - Line 47: `new_query = new_query % {key: tv[0] for key, tv in templates_and_values.items()}`
   - After SA compilation with pyformat, the SQL has `%(param_1)s LIKE %(param_2)s ESCAPE '\\'`
   - SA already doubles `%` to `%%` for pyformat (since `%` in ESCAPE is not a param marker)
   - Wait — actually SA might NOT double `%` in the ESCAPE clause. Need to verify.
   - The `escape_percent` replace might be handling exactly this case.

3. **Possible root cause candidates:**
   - SA compilation with pyformat generates correct SQL but `compile_mysql_query`'s manual `%` substitution on line 47 corrupts it
   - The formula test path goes through engine.execute() which handles `%` escaping differently than the async adapter path
   - The issue might be that `%%` in the compiled SQL (from SA escaping lone `%`) gets further mangled

### Next Steps
- Run a focused test locally to confirm the actual error traceback
- Check if SA's pyformat compilation doubles the `%` in ESCAPE clause
- Verify whether the test failure is in formula DB tests or in core/API tests
- Compare with MySQL test execution to find the exact divergence point
