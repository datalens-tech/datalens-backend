# StarRocks Connector - Known Issues & Future Work

**Last Updated**: 2026-03-12

---

## Known Issues

### 1. Missing `uuid` type mapping in StarRocksTypeTransformer (~9 test failures)

Complex query tests (`test_ago_any_db`, `test_triple_ago_any_db`, etc.) create sample tables with a `uuid` column. `StarRocksTypeTransformer.user_to_native_map` lacks `UserDataType.uuid`, causing `KeyError: <UserDataType.uuid: 9>`.

**Fix:** Add to `user_to_native_map` in `dl_connector_starrocks/core/type_transformer.py`:
```python
UserDataType.uuid: make_native_type(mysql_types.VARCHAR),
```
This matches the MySQL connector pattern.

### 2. Missing array type mappings in StarRocksTypeTransformer (~8 test failures)

Array tests (`test_array_contains_filter`, `test_array_contains_field`, etc.) fail with `KeyError` for `UserDataType.array_int`, `array_str`, `array_float`.

**Fix (may need iteration):** Add to `user_to_native_map`:
```python
UserDataType.array_int: make_native_type(mysql_types.TEXT),
UserDataType.array_str: make_native_type(mysql_types.TEXT),
UserDataType.array_float: make_native_type(mysql_types.TEXT),
```
StarRocks supports `ARRAY<T>` natively, but the SA type system uses MySQL dialect types. TEXT fallback may not be sufficient for actual array operations — if tests still fail, they should be skipped with reason `"StarRocks array types need dedicated SA type support"`.

### 3. `test_closing_sql_sessions` (1 failure)

aiomysql `ResultProxy` attempts cursor cleanup after event loop closure. This is an upstream driver issue in aiomysql.

### 4. Dependency Lint Issues (pre-existing)

- `dl_app_tools` is imported as a transitive dependency (DEP003)
- `dl-configs` is declared as a dependency but unused (DEP002)

---

## Resolved Issues (2026-03-12)

### uwsgi `--http` crash-loop on macOS/ARM (was blocking all 97 API tests)

Fixed by changing `--http` to `--http-socket` in `dl_core_testing/fixture_server_runner.py:160`. The uwsgi HTTP router gateway was crash-looping with a malloc overflow (`18446744073709551608` bytes). This was a shared infrastructure issue, not StarRocks-specific.

### Missing `raw_sql_level` in connection_params (was blocking ~26 tests)

Fixed by adding `raw_sql_level` passthrough in `dl_connector_starrocks_tests/db/api/base.py`, matching the MySQL connector pattern.

### Missing `data_caches_enabled` override (was blocking 1 test)

Fixed by setting `data_caches_enabled = True` on `TestStarRocksDataCache` to override MRO (where `ServiceFixtureTextClass.data_caches_enabled = False` was winning).

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
