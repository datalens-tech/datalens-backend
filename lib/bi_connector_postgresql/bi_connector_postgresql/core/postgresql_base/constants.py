from enum import Enum, unique


@unique
class PGEnforceCollateMode(Enum):
    auto = 'auto'
    on = 'on'
    off = 'off'
