from typing import Type

import sqlalchemy as sa
from sqlalchemy.types import TypeEngine

from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import DialectName


class SATypeConstructor:
    def get_sa_type(self, data_type: DataType) -> TypeEngine:
        raise NotImplementedError


class DefaultSATypeConstructor(SATypeConstructor):
    def get_sa_type(self, data_type: DataType) -> TypeEngine:
        type_map: dict[DataType, TypeEngine] = {
            **{
                str_type: sa.String(length=255)
                for str_type in (
                    DataType.STRING,
                    DataType.GEOPOINT,
                    DataType.GEOPOLYGON,
                )
            },
            DataType.INTEGER: sa.Integer(),
            DataType.FLOAT: sa.Float(),
            DataType.BOOLEAN: sa.Boolean(),
            DataType.DATE: sa.Date(),
            DataType.DATETIME: sa.DateTime(),
            DataType.GENERICDATETIME: sa.DateTime(),
            DataType.UUID: sa.String(length=255),
            DataType.ARRAY_INT: sa.ARRAY(sa.Integer),
            DataType.ARRAY_FLOAT: sa.ARRAY(sa.Float),
            DataType.ARRAY_STR: sa.ARRAY(sa.String),
        }
        return type_map[data_type]


_REGISTRY: dict[DialectName, Type[SATypeConstructor]] = {}


def get_type_constructor(dialect_name: DialectName) -> SATypeConstructor:
    type_constructor_cls = _REGISTRY[dialect_name]
    return type_constructor_cls()


def register_type_constructor(
    dialect_name: DialectName,
    type_constructor_cls: Type[SATypeConstructor],
) -> None:
    if dialect_name in _REGISTRY:
        assert _REGISTRY[dialect_name] is type_constructor_cls
    else:
        _REGISTRY[dialect_name] = type_constructor_cls
