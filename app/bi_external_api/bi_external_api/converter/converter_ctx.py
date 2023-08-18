from typing import Optional

import attr


@attr.s(frozen=True)
class ConverterContext:
    use_id_formula: Optional[bool] = attr.ib(default=False)
