from __future__ import annotations

import datetime
import json
import logging
from typing import Any, Collection, Optional

from bi_core.components.accessor import DatasetComponentAccessor

import bi_formula.core.nodes as formula_nodes
import bi_formula.shortcuts
from bi_formula.core.datatype import DataType

import bi_query_processing.exc
from bi_query_processing.enums import ExecutionLevel
from bi_query_processing.column_registry import ColumnRegistry
from bi_query_processing.compilation.primitives import (
    CompiledQuery, CompiledFormulaInfo, FromObject,
    AvatarFromObject, FromColumn, JoinedFromObject, BASE_QUERY_ID
)


LOGGER = logging.getLogger(__name__)


def make_joined_from_for_avatars(
        used_avatar_ids: Collection[str],
        ds_accessor: DatasetComponentAccessor,
        column_reg: ColumnRegistry,
        root_avatar_id: Optional[str] = None,
) -> JoinedFromObject:

    sorted_avatar_ids = sorted(used_avatar_ids)
    if root_avatar_id is None and sorted_avatar_ids:
        root_avatar_id = sorted_avatar_ids[0]

    froms: list[FromObject] = []
    added_avatar_ids: set[str] = set()
    for avatar_id in sorted_avatar_ids:
        assert avatar_id not in added_avatar_ids, f'Avatars should not be duplicated, but got {avatar_id} twice'
        avatar = ds_accessor.get_avatar_strict(avatar_id=avatar_id)
        columns = tuple(
            FromColumn(id=column.id, name=column.column.name)
            for column in column_reg.get_columns_for_avatar(avatar_id=avatar_id)
        )
        froms.append(
            AvatarFromObject(
                id=avatar_id, alias=avatar_id, avatar_id=avatar_id,
                source_id=avatar.source_id,
                columns=columns,
            )
        )
        added_avatar_ids.add(avatar_id)

    return JoinedFromObject(root_from_id=root_avatar_id, froms=froms)


def single_formula_comp_query_for_validation(
        formula: CompiledFormulaInfo,
        ds_accessor: DatasetComponentAccessor,
        column_reg: ColumnRegistry,
) -> CompiledQuery:
    return CompiledQuery(
        id=BASE_QUERY_ID,
        level_type=ExecutionLevel.source_db,
        select=[formula],
        group_by=[],
        order_by=[],
        filters=[],
        join_on=[],
        joined_from=make_joined_from_for_avatars(
            used_avatar_ids=formula.avatar_ids,
            ds_accessor=ds_accessor,
            column_reg=column_reg,
        ),
        limit=None, offset=None,
    )


def _datetime_fromisoformat(val: str) -> datetime.datetime:
    val = val.replace(' ', 'T')
    if val.endswith('Z'):
        val = val[:-1] + '+00:00'
    return datetime.datetime.fromisoformat(val)


ARRAY_TYPES = (
    DataType.ARRAY_STR, DataType.CONST_ARRAY_STR,
    DataType.ARRAY_INT, DataType.CONST_ARRAY_INT,
    DataType.ARRAY_FLOAT, DataType.CONST_ARRAY_FLOAT,
)
TREE_TYPES = (
    DataType.TREE_STR, DataType.CONST_TREE_STR,
)


def make_literal_node(val: Any, data_type: DataType) -> formula_nodes.BaseLiteral:
    """
    Make a ``formula_nodes.Literal`` from given value ``val`` converted to given type ``data_type``.
    For use in filters and parameter fields
    """
    node: Optional[formula_nodes.BaseLiteral] = None
    try:
        # strings can contain any of the types, so handle them separately
        if isinstance(val, str):
            if data_type in (DataType.DATE, DataType.CONST_DATE):
                dt_val = _datetime_fromisoformat(val).replace(tzinfo=None)
                if dt_val.hour or dt_val.minute or dt_val.second or dt_val.microsecond:
                    LOGGER.warning('Truncating datetime with nonzero time to date: %s', val)
                node = formula_nodes.LiteralDate.make(dt_val.date())
            elif data_type in (DataType.DATETIME, DataType.CONST_DATETIME):
                # NOTE: the value might have non-empty tzinfo.
                # TODO: use LiteralDatetimeTZ for filtering DATETIME values. https://st.yandex-team.ru/BI-2384
                node = formula_nodes.LiteralDatetime.make(_datetime_fromisoformat(val))
            elif data_type in (DataType.DATETIMETZ, DataType.CONST_DATETIMETZ):
                val_dt = _datetime_fromisoformat(val)
                # Incoming offset-less datetimes are interpreted as UTC
                if val_dt.tzinfo is None:
                    val_dt = val_dt.replace(tzinfo=datetime.timezone.utc)
                node = formula_nodes.LiteralDatetimeTZ.make(val_dt)
            elif data_type in (DataType.GENERICDATETIME, DataType.CONST_GENERICDATETIME):
                node = formula_nodes.LiteralGenericDatetime.make(_datetime_fromisoformat(val))
            elif data_type in (DataType.INTEGER, DataType.CONST_INTEGER):
                node = formula_nodes.LiteralInteger.make(int(val))
            elif data_type in (DataType.FLOAT, DataType.CONST_FLOAT):
                node = formula_nodes.LiteralFloat.make(float(val))
            elif data_type in (DataType.BOOLEAN, DataType.CONST_BOOLEAN):
                bool_val = {'true': True, 'false': False}.get(val.lower())
                if bool_val is None:
                    raise ValueError('Invalid value for bool')
                node = formula_nodes.LiteralBoolean.make(bool_val)
            elif data_type in (DataType.STRING, DataType.CONST_STRING):
                node = formula_nodes.LiteralString.make(val)
            elif data_type in (DataType.GEOPOINT, DataType.CONST_GEOPOINT):
                node = formula_nodes.LiteralGeopoint.make(val)
            elif data_type in (DataType.GEOPOLYGON, DataType.CONST_GEOPOLYGON):
                node = formula_nodes.LiteralGeopolygon.make(val)
            elif data_type in (DataType.UUID, DataType.CONST_UUID):
                node = formula_nodes.LiteralUuid.make(val)
            elif data_type in ARRAY_TYPES or data_type in TREE_TYPES:
                try:
                    val = json.loads(val)
                except json.decoder.JSONDecodeError:
                    raise ValueError('Invalid value for array')
            # No known use-cases: DataType.MARKUP
            else:
                raise ValueError('Unexpected data_type value')

        if data_type in ARRAY_TYPES or data_type in TREE_TYPES:
            if not isinstance(val, list):
                raise ValueError('Got non-list for array')
            if data_type in (DataType.ARRAY_STR, DataType.CONST_ARRAY_STR):
                node = formula_nodes.LiteralArrayString.make(val)
            elif data_type in (DataType.ARRAY_INT, DataType.CONST_ARRAY_INT):
                node = formula_nodes.LiteralArrayInteger.make(val)
            elif data_type in (DataType.ARRAY_FLOAT, DataType.CONST_ARRAY_FLOAT):
                node = formula_nodes.LiteralArrayFloat.make(val)
            elif data_type in (DataType.TREE_STR, DataType.CONST_TREE_STR):
                node = formula_nodes.LiteralTreeString.make(val)

        if node is None:
            node = bi_formula.shortcuts.n.lit(val)  # guess literal type

    except (ValueError, TypeError):
        raise bi_query_processing.exc.InvalidLiteralError(f'Invalid literal value {val!r} for type {data_type.name}')

    assert node is not None
    return node
