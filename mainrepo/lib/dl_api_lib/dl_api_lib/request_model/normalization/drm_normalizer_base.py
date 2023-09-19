import abc
from typing import (
    Generic,
    TypeVar,
)

from dl_api_lib.query.formalization.id_gen import IdGenerator
from dl_api_lib.query.formalization.raw_specs import RawQuerySpecUnion
from dl_api_lib.request_model.data import DataRequestModel


_SPEC_TV = TypeVar("_SPEC_TV")


class RequestPartSpecNormalizerBase(abc.ABC, Generic[_SPEC_TV]):
    def get_liid_generator(self, raw_query_spec_union: RawQuerySpecUnion) -> IdGenerator:
        used_ids = {
            spec.legend_item_id for spec in raw_query_spec_union.iter_item_specs() if spec.legend_item_id is not None
        }
        id_gen = IdGenerator(used_ids=used_ids)
        return id_gen

    def normalize_spec(
        self,
        spec: _SPEC_TV,
        raw_query_spec_union: RawQuerySpecUnion,
    ) -> tuple[_SPEC_TV, RawQuerySpecUnion]:
        return spec, raw_query_spec_union


_DRM_TV = TypeVar("_DRM_TV", bound=DataRequestModel)


class RequestModelNormalizerBase(abc.ABC, Generic[_DRM_TV]):
    """
    The idea behind this class is that it expands various API shortcuts
    into a full shortcut-less version of the request
    (e.g. converts the short pivot totals spec into additional
    legend items and pivot structure items).
    """

    def normalize_drm(self, drm: _DRM_TV) -> _DRM_TV:
        return drm
