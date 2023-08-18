from __future__ import annotations

from typing import Any, Dict, List, Optional

import attr


@attr.s(frozen=True)
class ApiResponse:
    pass


@attr.s(frozen=True)
class HttpApiResponse(ApiResponse):
    """
    Client-independent BI HTTP API response object
    """

    _status_code: int = attr.ib(kw_only=True)
    _json: Dict[str, Any] = attr.ib(kw_only=True)

    @property
    def status_code(self) -> int:
        return self._status_code

    @property
    def bi_status_code(self) -> Optional[str]:
        return self.json.get('code')

    @property
    def json(self) -> dict:
        return self._json

    @property
    def response_errors(self) -> List[str]:
        return self.extract_response_errors(self._json)

    @classmethod
    def extract_response_errors(cls, response_json: Dict[str, Any]) -> List[str]:
        return []


class ApiBase:
    pass
