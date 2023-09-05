from __future__ import annotations

import uuid
from typing import Optional


def make_uuid_from_parts(current: str, parent: Optional[str]) -> str:
    assert current

    if not parent:
        return current

    uuid_maxlen = 120
    uuid_sep = '--'  # need to be a non-word character, to get successfully separated by elasticsearch
    cutted_replace = '...'

    if len(parent) + len(current) > uuid_maxlen:
        cutted_half_len = int((uuid_maxlen - len(current) - len(cutted_replace)) / 2)
        cutted_parent = parent[:cutted_half_len] + cutted_replace + parent[-cutted_half_len:]
        parent = cutted_parent

    result = parent + uuid_sep + current
    return result


def request_id_generator(prefix: Optional[str] = None) -> str:
    result = uuid.uuid4().hex
    if prefix is not None:
        result = prefix + '.' + result
    return result
