import abc
from importlib import metadata
from typing import (
    Collection,
    Generic,
    Optional,
    TypeVar,
)

import attr


_EP_CLS_TV = TypeVar("_EP_CLS_TV")


@attr.s
class EntrypointClassManager(abc.ABC, Generic[_EP_CLS_TV]):
    """
    Loads classes registered as entrypoints of a given type
    """

    entrypoint_group_name: str = attr.ib(kw_only=True)

    def get_all_ep_classes(self, ep_filter: Optional[Collection[str]] = None) -> dict[str, type[_EP_CLS_TV]]:
        entrypoints = list(metadata.entry_points().select(group=self.entrypoint_group_name))
        if ep_filter is not None:
            entrypoints = [ep for ep in entrypoints if ep.name in ep_filter]
        return {ep.name: ep.load() for ep in entrypoints}

    def get_ep_class(self, name: str) -> type[_EP_CLS_TV]:
        entrypoints = list(metadata.entry_points().select(group=self.entrypoint_group_name, name=name))
        assert len(entrypoints) == 1
        return entrypoints[0].load()
