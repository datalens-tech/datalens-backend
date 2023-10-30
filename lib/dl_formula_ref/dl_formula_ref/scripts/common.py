from dl_formula_ref.config import ConfigVersion


def conf_version_type(s: str) -> ConfigVersion:
    return ConfigVersion[s]
