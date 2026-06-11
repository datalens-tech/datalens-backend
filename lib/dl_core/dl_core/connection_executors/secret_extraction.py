from dl_core.base_models import BaseAttrsDataModel


def data_model_secret_fields(cls: type) -> frozenset[str]:
    """Resolver for get_secret_strings's extra_secret_fields.

    For BaseAttrsDataModel subclasses, returns the set of top-level field names
    declared as secret via get_secret_keys(). Other classes return an empty set.

    Multi-part DataKeys (len(parts) > 1) are handled by walker recursion: when
    the walker enters a nested attrs class, it asks this resolver about that
    class's own get_secret_keys().
    """
    if issubclass(cls, BaseAttrsDataModel):
        return frozenset(key.parts[0] for key in cls.get_secret_keys() if len(key.parts) == 1)
    return frozenset()
