# dl_obfuscator

Log/trace/Sentry payload obfuscation utilities. The package owns:

- `ObfuscationEngine` + base obfuscators (`SecretObfuscator`, `RegexObfuscator`) — the redaction pipeline applied to outbound payloads (logs, Sentry, tracing, usage tracking, inspector).
- `SecretKeeper` — a registry mapping known secret strings to human-readable names, consumed by `SecretObfuscator`.
- `get_secret_strings(settings)` — a settings-tree walker that discovers every secret string under `repr=False` fields and feeds them into a `SecretKeeper` at app startup.

## ⚠️ `repr=False` is a security marker

In this codebase `repr=False` on any settings field — pydantic `Field(repr=False)` or attrs `attr.ib(repr=False)` — is **the source of truth** for "this value is a secret". `get_secret_strings(...)` walks the settings tree, finds every string and bytes leaf living under a `repr=False` subtree, and registers them in the global `SecretKeeper` so log obfuscation can redact them.

Practical consequences:

 - **Always set `repr=False` on every secret field**, even if you'd otherwise rely on `model_dump()` / `__repr__` never being called. Anything missing `repr=False` will leak into logs (and very short secrets may still be skipped by `SecretKeeper`'s minimum-length filter).
- **Never set `repr=False` for cosmetic reasons.** Marking a non-secret field hides it from logs and silently inflates the secret registry (every value gets redacted globally on substring match).

### Propagation: `repr=False` is inherited downward

Once the walker enters a `repr=False` subtree it treats **every** descendant string/bytes leaf as secret, regardless of any `repr` values on children. Example:

```python
class Inner(pydantic.BaseModel):
    user: str            # not marked repr=False…
    password: str        # …nor this one

class Outer(pydantic.BaseModel):
    inner: Inner = pydantic.Field(repr=False)   # …but Outer.inner is, so
                                                # inner.user AND inner.password
                                                # both land in SecretKeeper.
```

Design the boundary at the right level: put `repr=False` exactly where secrecy actually starts. Marking an entire `AuthSettings` block secret is fine if every leaf inside it is genuinely secret; otherwise narrow the scope to the actually-secret fields.

## `get_secret_strings`

```python
from dl_obfuscator import SecretKeeper, get_secret_strings

keeper = SecretKeeper()
# get_secret_strings returns dict[dotted_path, value]; add_secrets bulk-registers it.
keeper.add_secrets(get_secret_strings(settings))
```

Supported containers / nodes:

- pydantic `BaseModel` (recurses by field; `repr=False` on a field flips `in_secret`)
- attrs classes incl. `slots=True` (recurses by field; `repr=False` on a field flips `in_secret`) — also handles the legacy fallback subtree on `dl_settings.BaseRootSettingsWithFallback`
- `dict[K, V]` (recurses by value, path is `[key]`)
- `tuple`, `list` (recurses by element, path is `[index]`)
- `set`, `frozenset` (recurses by element, path is `[index]`; iteration order is **not** stable across processes)
- `str` leaves → emitted if inside a `repr=False` subtree
- `bytes` leaves → decoded via utf-8 with replacement and emitted if inside a `repr=False` subtree
- `None`, numbers, enums, and other non-str leaves are skipped

Unsupported container types (e.g. user-defined classes, custom collections) are **not silently swallowed**: when encountered inside a `repr=False` subtree the walker emits a `WARNING` log via `dl_obfuscator.secret_walker` so the leak is observable. If you hit a warning, either give the value a known container shape or extend the walker.

## Per-request seeding & `extra_secret_fields`

`get_secret_strings` also accepts `extra_secret_fields: Callable[[type], frozenset[str]]` — a resolver that marks extra field names secret beyond `repr=` (e.g. `BaseAttrsDataModel.get_secret_keys()`). Per-request middleware and connection-executor code seed the request-scoped `SecretKeeper` from runtime objects (auth data, connection data models, target DTOs), passing a source prefix:

```python
keeper.add_secrets(
    get_secret_strings(conn.data, extra_secret_fields=data_model_secret_fields),
    prefix="connection",
)
```

`add_secrets(secrets, prefix="connection")` registers each secret under `connection.<path>`, so redacted log lines identify the source (e.g. `***connection.password***`).
