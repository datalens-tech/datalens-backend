import abc

from dl_api_lib.query.formalization.id_gen import IdGenerator
from dl_api_lib.query.formalization.raw_specs import RawQuerySpecUnion
from dl_api_lib.request_model.data import DataRequestModel


class RequestPartSpecNormalizerBase[SPEC_TV](abc.ABC):
    def get_liid_generator(self, raw_query_spec_union: RawQuerySpecUnion) -> IdGenerator:
        used_ids = {
            spec.legend_item_id for spec in raw_query_spec_union.iter_item_specs() if spec.legend_item_id is not None
        }
        id_gen = IdGenerator(used_ids=used_ids)
        return id_gen

    def normalize_spec(
        self,
        spec: SPEC_TV,
        raw_query_spec_union: RawQuerySpecUnion,
    ) -> tuple[SPEC_TV, RawQuerySpecUnion]:
        return spec, raw_query_spec_union


class RequestModelNormalizerBase[DRM_TV: DataRequestModel](abc.ABC):
    """
    The idea behind this class is that it expands various API shortcuts
    into a full shortcut-less version of the request
    (e.g. converts the short pivot totals spec into additional
    legend items and pivot structure items).
    """

    def normalize_drm(self, drm: DRM_TV) -> DRM_TV:
        return drm
