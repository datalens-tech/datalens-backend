from collections.abc import Callable
import time
from typing import (
    Any,
)


def delay_rerun_filter(delay_seconds: int = 10) -> Callable[..., bool]:
    def _rerun_filter(*args: Any, **kwargs: Any) -> bool:
        time.sleep(delay_seconds)
        return True

    return _rerun_filter
