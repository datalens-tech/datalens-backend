from typing import Any

from clickhouse_sqlalchemy.engines import Engine


class YtTable(Engine):
    def __init__(self, **kwargs: Any):
        self.engine_kwargs = kwargs
        super().__init__()

    def get_parameters(self) -> str:  # Method should return something stringable
        if not self.engine_kwargs:
            return ""
        content_str = ",".join([f"{key}={value}" for key, value in sorted(self.engine_kwargs.items())])
        return f"{{{content_str}}}"
