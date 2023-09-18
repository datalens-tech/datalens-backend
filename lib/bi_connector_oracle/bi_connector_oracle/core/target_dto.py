from __future__ import annotations

from typing import Type, TypeVar, TYPE_CHECKING

import attr

from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO

from bi_connector_oracle.core.constants import OracleDbNameType

if TYPE_CHECKING:
    from dl_constants.types import TJSONLike


_CT_DTO_TV = TypeVar("_CT_DTO_TV", bound='OracleConnTargetDTO')


@attr.s(frozen=True)
class OracleConnTargetDTO(BaseSQLConnTargetDTO):
    db_name_type: OracleDbNameType = attr.ib()

    def to_jsonable_dict(self) -> dict[str, TJSONLike]:
        d = super().to_jsonable_dict()
        return {
            **d,
            'db_name_type': self.db_name_type.name,
        }

    @classmethod
    def _from_jsonable_dict(cls: Type[_CT_DTO_TV], data: dict) -> _CT_DTO_TV:
        prepared_data = {
            **data,
            'db_name_type': OracleDbNameType[data['db_name_type']]
        }
        return cls(**prepared_data)  # type: ignore
