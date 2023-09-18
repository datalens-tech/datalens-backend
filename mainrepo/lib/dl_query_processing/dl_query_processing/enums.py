from __future__ import annotations

from enum import (
    Enum,
    unique,
)


@unique
class QueryType(Enum):
    result = "result"
    preview = "preview"
    distinct = "distinct"  # FIXME: value_distinct
    totals = "totals"
    value_range = "value_range"
    pivot = "pivot"


@unique
class SelectValueType(Enum):
    plain = "plain"  # Select as-is, without any modifiers (default for most cases)
    max = "max"  # Wrap in MAX
    min = "min"  # Wrap in MIN
    literal = "literal"  # Select an arbitrary literal instead
    null = "null"  # Select `NULL` instead
    array_prefix = "array_prefix"  # Select `NULL` instead


@unique
class EmptyQueryMode(Enum):
    error = "error"  # Raise error on empty query
    empty = "empty"  # Return empty data on empty query
    empty_row = "empty_row"  # Return single empty row on empty query


@unique
class GroupByPolicy(Enum):
    force = "force"  # Enable and force GROUP BY
    disable = "disable"  # Disable Group BY
    if_measures = "if_measures"  # Enable only if the query contains measures, otherwise disable


@unique
class ProcessingStage(Enum):
    """
    Stages of field formula processing
    """

    base = "base"
    dep_generation = "dep_generation"
    pre_sub_mutation = "pre_sub_mutation"
    substitution = "substitution"
    casting = "casting"
    aggregation = "aggregation"
    mutation = "mutation"
    validation = "validation"
    final = "final"


@unique
class QueryPart(Enum):
    select = "select"
    group_by = "group_by"
    filters = "filters"
    order_by = "order_by"
    join_on = "join_on"


@unique
class ExecutionLevel(Enum):
    source_db = "source_db"
    compeng = "compeng"
