# dl-cls

Column-Level Security (CLS) masking engine for DataLens.

Where RLS (`dl_rls`) hides **rows** by rewriting SQL before the query runs, CLS masks
**column values** in postprocessing, after SQL has run. This package is the standalone,
dependency-light engine that later tickets wire into datasets and data-api: data models,
deterministic masking transforms, and a pure effective-rule selector. It is not yet imported
by any consumer.

## What's inside

- **Models** (`dl_cls.models`) — frozen, deeply hashable `CLSMaskSpec`, `CLSSubject`, `CLSRule`,
  `FieldCLS`. `FieldCLS` is hashable so it can live inside a `BIField` cache key.
- **Masking transforms** (`make_masker`) — build a `Callable[[str | None], str | None]` for a spec.
- **Effective-rule selection** (`select_effective_rule`) — a pure function choosing the spec to
  apply for a request, with no request-context object.
- **Persistence schema** (`dl_cls.schema`) — marshmallow schemas (built on the `dl_model_tools`
  base) for the structured US-storage form.
- **Exceptions** (`CLSError`).

## Core concepts

**Masking modes** (`CLSMode`, increasing strictness `none < partial < full`):

| Mode | Behavior |
|------|----------|
| `none` | value passed through unchanged |
| `partial` | keep the first `prefix` and last `suffix` chars, replace the middle with `mask`; `keep_length=True` fills so the output length equals the input length |
| `full` | replace the whole value with `mask` (default `"******"`); `keep_length=True` repeats the mask to the value's length so the output length is preserved |

`None` always passes through unchanged. A value no longer than `prefix + suffix` (or, without
length preservation, no longer than the mask) is replaced wholesale — source characters never leak.

**Subjects** (`CLSSubjectType`) are `user` or `group` only. There is **no `all` subject**: the
"applies to everyone" role is filled by a field's **default rule**.

**Effective-rule selection** walks strict tiers — concrete `user` → `group` → `default_rule` —
and within the first non-empty subject tier returns the **strictest** spec. Strictness is the mode
map first; within `partial` and `full`, a spec without length preservation outranks one with it,
and (for `partial`, which has edges) fewer revealed edge characters outranks more. A field with no matching rule falls back to `default_rule`
(`full` by default), never to an unmasked result.

## Usage

```python
from dl_cls import CLSMaskSpec, CLSRule, CLSSubject, FieldCLS, make_masker, select_effective_rule
from dl_constants.enums import CLSMode, CLSSubjectType

field_cls = FieldCLS(
    default_rule=CLSMaskSpec(mode=CLSMode.full),  # everyone else: fully masked
    rules=[
        CLSRule(
            subject=CLSSubject(subject_type=CLSSubjectType.user, subject_id="manager"),
            spec=CLSMaskSpec(mode=CLSMode.none),  # this user sees the raw value
        ),
    ],
)

spec = select_effective_rule(field_cls, user_id="someone", allowed_groups=set())
masker = make_masker(spec)
masker("john@example.com")  # -> "******"
```

Persistence round-trip:

```python
from dl_cls.schema import FieldCLSSchema

schema = FieldCLSSchema()
payload = schema.dump(field_cls)
restored = schema.load(payload)  # == field_cls
```

The schema requires `default_rule` whenever `rules` is non-empty: a field that configures any CLS
rule must declare its everyone-policy explicitly.

## Tests

```bash
task dev:test       # unit tests (no docker compose)
task dev:lint-fix   # black / isort / ruff / toml-sort / deptry / mypy
```
