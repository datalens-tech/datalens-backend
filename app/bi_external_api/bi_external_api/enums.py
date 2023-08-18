import enum


class ExtAPIType(enum.Enum):
    # default api type, to be assumed by default and should not be used to mark particular model
    CORE = "core"
    DC = "dc"
    UNIFIED_DC = "unified_il"
    UNIFIED_NEBIUS_IL = "unified_nebius_il"
    YA_TEAM = "ya-team"
