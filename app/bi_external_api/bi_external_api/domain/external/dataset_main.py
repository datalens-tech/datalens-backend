from typing import Sequence, Optional

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor, AttribDescriptor
from bi_external_api.attrs_model_mapper.utils import MText
from bi_external_api.domain.external.avatar import AvatarsConfig
from bi_external_api.domain.external.data_source import DataSource
from bi_external_api.domain.external.dataset_field import DatasetField
from bi_external_api.domain.utils import ensure_tuple


@ModelDescriptor()
@attr.s(frozen=True)
class Dataset:
    fields: Sequence[DatasetField] = attr.ib(converter=ensure_tuple)
    avatars: Optional[AvatarsConfig] = attr.ib(metadata=AttribDescriptor(
        description=MText(
            ru="Конфигурация аватаров (аналог секции `FROM` в SQL)."
                " Для одного источника данных можно передавать значение `null`,"
                " тогда аватар создастся автоматически и ID автара будет равен ID источника данных.",
            en="Avatar configuration (analogous to `FROM` section in SQL)."
                " For a single data source you can send the `null` value,"
                " in this case, an avatar will be created automatically and its ID will be equal to data source ID.",
            )
        ).to_meta()
    )
    sources: Sequence[DataSource] = attr.ib(converter=ensure_tuple)

    @property
    def strict_avatars(self) -> AvatarsConfig:
        avatars = self.avatars
        assert avatars is not None
        return avatars
