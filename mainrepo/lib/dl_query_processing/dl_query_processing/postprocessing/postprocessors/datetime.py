from __future__ import annotations

import datetime
from typing import Optional

import pytz


def postprocess_datetime(value: datetime.datetime) -> Optional[str]:
    if value is None:
        return value

    if isinstance(value, str):
        value = datetime.datetime.fromisoformat(value)
    if value.tzinfo is not None:
        value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)

    return value.isoformat()


def postprocess_genericdatetime(value: datetime.datetime) -> Optional[str]:
    if value is None:
        return value

    if isinstance(value, str):
        value = datetime.datetime.fromisoformat(value)

    return value.replace(tzinfo=None).isoformat()


def make_postprocess_datetimetz(tzname: str):  # type: ignore  # TODO: fix
    tzobj = pytz.timezone(tzname)

    def _postprocess_datetimetz(value: datetime.datetime, tzobj=tzobj) -> Optional[str]:  # type: ignore  # TODO: fix
        if value is None:
            return value

        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        return value.astimezone(tzobj).isoformat()

    return _postprocess_datetimetz
