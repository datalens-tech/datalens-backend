"""
Assorted methods for making a safe representative value out of some
less constrained value.
"""

from __future__ import annotations

import base64
import hashlib
import json
import random
import re
import string
from typing import (
    Any,
    Callable,
    Iterable,
    Tuple,
)
import uuid

CHAR_CLASSES = {
    "H": set(string.hexdigits.upper()),
    "h": set(string.hexdigits.lower()),
    "-": {"-"},
}
# '-'.join('h' * x for x in (8, 4, 4, 4, 12))
UUID_LOWER = "hhhhhhhh-hhhh-hhhh-hhhh-hhhhhhhhhhhh"
UUID_UPPER = UUID_LOWER.upper()
UUID_LOWER_CLASSES = tuple(CHAR_CLASSES[char] for char in UUID_LOWER)
UUID_UPPER_CLASSES = tuple(CHAR_CLASSES[char] for char in UUID_UPPER)


def cut_uuid(value: str) -> Tuple[str, str]:
    """
    Cut as much of an uuid-looking prefix from `value` as possible.

    >>> some_uuid = '765a18f2-4d1c-4a7f-b0cd-8bdb392803f7'
    >>> cut_uuid(some_uuid.lower())
    ('765a18f2-4d1c-4a7f-b0cd-8bdb392803f7', '')
    >>> cut_uuid(some_uuid.upper())
    ('765A18F2-4D1C-4A7F-B0CD-8BDB392803F7', '')
    >>> cut_uuid('')
    ('', '')
    >>> cut_uuid('zxc')
    ('', 'zxc')
    >>> cut_uuid(some_uuid.upper()[:10] + some_uuid.lower()[10:])
    ('765A18F2-4', 'd1c-4a7f-b0cd-8bdb392803f7')
    """
    idx = max(
        next(
            # basically enumerate(zip(value, ccs))
            (idx for idx in range(min(len(value), len(ccs))) if value[idx] not in ccs[idx]),
            len(value),
        )
        for ccs in (UUID_LOWER_CLASSES, UUID_UPPER_CLASSES)
    )
    return value[:idx], value[idx:]


def string_to_uuid(
    data: str,
    min_random_bytes: int = 3,
    visual_separate: str = "ff",
) -> str:
    """
    Make an UUID string out of any string,
    with some of its data and some random bits.

    >>> string_to_uuid(string_to_uuid('765a18f2-4d1f-f63f-fc3b-2267eceb5182'))[:-6]
    '765a18f2-4d1f-f63f-fc3b-22ffff'
    """
    data_uuid_like, data_etcetera = cut_uuid(data)
    uuid_chars = 32

    char_budget = uuid_chars
    # 0: data_uuid_hex
    # 1: visual separator
    # 2: data-bytes
    # 3: visual separator
    # 4: random hex padding
    # 5: random hex (required)
    hex_pieces = {}

    if visual_separate:
        hex_pieces[1] = visual_separate
        char_budget -= len(visual_separate)
        hex_pieces[3] = visual_separate
        char_budget -= len(visual_separate)

    def make_random_hex(chars: int) -> str:
        return "%0{}x".format(chars) % (random.getrandbits(chars * 4),)

    random_hex_chars = int(min_random_bytes * 2)
    random_hex_tail = make_random_hex(random_hex_chars)
    hex_pieces[5] = random_hex_tail
    char_budget -= len(random_hex_tail)
    assert char_budget >= 0, "should not ask for too many random bytes"

    if char_budget > 0:
        data_uuid_hex = data_uuid_like.replace("-", "")
        data_uuid_hex = data_uuid_hex[:char_budget]
        hex_pieces[0] = data_uuid_hex
        char_budget -= len(data_uuid_hex)

    if char_budget > 0:
        max_bytechars = int(char_budget / 2 + 1)
        data_etcetera_hex = "".join("%02x" % (ord(char),)[:2] for char in data_etcetera[:max_bytechars])
        data_etcetera_hex = data_etcetera_hex[:char_budget]
        hex_pieces[2] = data_etcetera_hex
        char_budget -= len(data_etcetera_hex)

    if char_budget > 0:
        random_hex_padding = make_random_hex(char_budget)
        hex_pieces[4] = random_hex_padding
        char_budget -= len(random_hex_padding)

    assert char_budget == 0, dict(char_budget=char_budget, hex_pieces=hex_pieces)
    assert sum(len(piece) for piece in hex_pieces.values()) == uuid_chars
    result_hex = "".join(hex_pieces.get(idx, "") for idx in range(6))
    return str(uuid.UUID(hex=result_hex))


# `Any` should be a `hashlib._hashlib.HASH`,
# an object with a `def digest(self) -> bytes:` method.
THasher = Callable[[bytes], Any]


def clear_hash(
    value: str,
    lowercase: bool = True,
    allowed_chars_re: str = "A-Za-z0-9_",
    hash_prefix: str = "cs",
    base_len: int = 12,
    hash_len: int = 12,
    hasher: THasher = hashlib.sha256,
) -> str:
    """
    Hash a value, leaving a sanitized part of it for debuggability.

    >>> clear_hash('<a href="javascript:alert(2)">фыва</a>')
    'a_href_javas__cst2let6ipy3jh'
    """
    hashed = hasher(value.encode("utf-8")).digest()
    hashed = base64.b32encode(hashed).decode("ascii").lower().rstrip("=")
    hashed = hashed[:hash_len]

    cleaned = re.sub(r"[^{}]+".format(allowed_chars_re), "_", value)
    cleaned = cleaned.strip("_")
    cleaned = cleaned[:base_len]
    return f"{cleaned}__{hash_prefix}{hashed}"


def make_jsonable(value: Any, default: Callable[[Any], Any] = repr) -> Any:
    """
    A hack to ensure the value is JSON-compatible.
    To be used in logging `extra` in uncertain cases.
    """
    return json.loads(json.dumps(value, default=default))


def param_bool(value: Any, false_values: Iterable[str] = ("false", "0", "no"), default: bool = False) -> bool:
    """
    Header value to a boolean value.

    Note: OpenAPI spec only allows 'true' and 'false'.
    """
    if isinstance(value, bool):
        return value
    if not value:
        return default
    value = value.lower()
    if value in false_values:
        return False
    return True
