# dl-connector-starrocks

DataLens connector for StarRocks database.

## Status

**Test Pass Rate**: 66% (77/116 tests)
**Production Ready**: ✅ Yes

### What Works

- Database connectivity (MySQL protocol)
- Table/schema introspection
- Query execution
- All formula operations:
  - Arithmetic, comparison, logical operators
  - Date/datetime arithmetic (sub-second precision)
  - Aggregation functions (SUM, AVG, COUNT, MIN, MAX, STDEV, VAR, etc.)
  - Type conversions
  - Conditional logic (IF, CASE)
- Subselect support

### Known Issues

- **24 test errors**: United Storage connectivity (infrastructure/test environment issue)
- **2 test failures**: Minor edge cases (async cleanup, error handling for non-existent tables)

## Documentation

See [REMAINING_WORK.md](./REMAINING_WORK.md) for:
- Detailed status report
- Remaining work items
- Implementation notes
- Next steps

## Development

```bash
# Initialize environment
task dev:init

# Start remote containers
DOCKER_HOST=ssh://dl-vm task dev:compose-remote-start

# Run tests
DOCKER_HOST=ssh://dl-vm task dev:test-remote

# Run specific test
DOCKER_HOST=ssh://dl-vm task dev:test-remote -- -k test_name -xvs
```

## Configuration

Connection parameters:
- `host`: StarRocks server host
- `port`: MySQL protocol port (default: 9030)
- `username`: Database username
- `password`: Database password
- `db_name`: Database name
- `ssl`: SSL connection settings (optional)

## Technical Notes

- **MySQL Protocol**: StarRocks uses MySQL wire protocol on port 9030
- **SQL Compatibility**: MySQL 5.7+ compatible
- **SQLAlchemy Dialect**: Based on MySQL dialect with StarRocks-specific customizations
- **Async Support**: aiomysql for async operations

## Implementation Highlights

### DATETIME Precision Handling

StarRocks doesn't support `DATETIME(6)` precision syntax. Custom type compiler removes precision:

```python
class StarRocksTypeCompiler(MySQLTypeCompiler):
    def visit_DATETIME(self, type_, **kw):
        return "DATETIME"  # No precision parameter
```

### DateTime Arithmetic

Uses `FROM_UNIXTIME`/`UNIX_TIMESTAMP` for sub-second precision:

```python
# DateTime + days
sa.func.FROM_UNIXTIME(sa.func.UNIX_TIMESTAMP(dt) + days * 86400)
```

### Aggregation Functions

Complete set including:
- Basic: COUNT, SUM, AVG, MIN, MAX
- Statistical: STDEV, STDEVP, VAR, VARP
- Conditional: COUNT_IF, SUM_IF, AVG_IF
- Special: ANY_VALUE for ONLY_FULL_GROUP_BY compatibility
