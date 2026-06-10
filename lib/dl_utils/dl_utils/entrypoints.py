from collections.abc import Collection
from importlib import metadata

import attr


@attr.s
class EntrypointClassManager[EP_CLS_TV]:
    """
    Loads classes registered as entrypoints of a given type
    """

    entrypoint_group_name: str = attr.ib(kw_only=True)

    def get_all_ep_classes(self, ep_filter: Collection[str] | None = None) -> dict[str, type[EP_CLS_TV]]:
        entrypoints = list(metadata.entry_points().select(group=self.entrypoint_group_name))
        if ep_filter is not None:
            entrypoints = [ep for ep in entrypoints if ep.name in ep_filter]
        return {ep.name: ep.load() for ep in entrypoints}

    def get_ep_class(self, name: str) -> type[EP_CLS_TV]:
        entrypoints = list(metadata.entry_points().select(group=self.entrypoint_group_name, name=name))
        assert len(entrypoints) == 1
        return entrypoints[0].load()
