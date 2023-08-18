from __future__ import annotations

import json
from typing import Optional
from functools import lru_cache


DUMMY_GEO_VALUE = '__DUMMY_GEO_VALUE__'


@lru_cache(maxsize=2 ** 10)
def postprocess_geopoint(value) -> Optional[str]:  # type: ignore  # TODO: fix
    value = str(value)
    if value == DUMMY_GEO_VALUE:
        return None
    try:
        geopoint = json.loads(value)
        if not isinstance(geopoint, list) or not len(geopoint) == 2:
            return None
        if not isinstance(geopoint[0], (int, float)) or not (-90 <= geopoint[0] <= 90):
            return None
        if not isinstance(geopoint[1], (int, float)) or not (-180 <= geopoint[1] <= 180):
            return None
    except ValueError:
        return None
    return value


# geopoint_re = re.compile(r'\[-?([0-9]|[1-8][0-9]|90)(\.\d+)?,\s*-?([0-9]|[1-9][0-9]|1[1-7][0-9]|180)(\.\d+)?\]')
#
#
# def postprocess_geopoint_re(value) -> str:
#     """
#     In [61]: %timeit postprocess_geopoint_re('[55.832995, -37.634928]')
#     1.14 µs ± 33.3 ns per loop (mean ± std. dev. of 7 runs, 1000000 loops each)
#
#     In [63]: %timeit postprocess_geopoint('[55.832995, -37.634928]')
#     3.75 µs ± 252 ns per loop (mean ± std. dev. of 7 runs, 100000 loops each)
#     """
#
#     value = str(value)
#     if geopoint_re.match(value):
#         return value
#     else:
#         return None


@lru_cache(maxsize=2 ** 10)
def postprocess_geopolygon(value) -> Optional[str]:  # type: ignore  # TODO: fix
    value = str(value)
    if value == DUMMY_GEO_VALUE:
        return None
    try:
        multipolygon = json.loads(value)
        if not isinstance(multipolygon, list) or not multipolygon:
            return None
        for sub_polygon in multipolygon:
            if not isinstance(sub_polygon, list) or not sub_polygon:
                return None
            for geopoint in sub_polygon:
                if not isinstance(geopoint, list) or not len(geopoint) == 2:
                    return None
                if not isinstance(geopoint[0], (int, float)) or not (-90 <= geopoint[0] <= 90):
                    return None
                if not isinstance(geopoint[1], (int, float)) or not (-180 <= geopoint[1] <= 180):
                    return None
    except ValueError:
        return None
    return value


# geopolygon_re = re.compile(
#     r'\s*\[\s*(\[\s*(\[\s*'
#     r'-?([0-9]|[1-8][0-9]|90)(\.\d+)?\s*,\s*'
#     r'-?([0-9]|[1-9][0-9]|1[1-7][0-9]|180)(\.\d+)?\s*'
#     r'\]\s*,\s*)*'
#     r'\[\s*-?([0-9]|[1-8][0-9]|90)(\.\d+)?\s*,\s*'
#     r'-?([0-9]|[1-9][0-9]|1[1-7][0-9]|180)(\.\d+)?\s*'
#     r'\]\s*\]\s*,\s*)*'
#     r'\[\s*(\[\s*'
#     r'-?([0-9]|[1-8][0-9]|90)(\.\d+)?\s*,\s*'
#     r'-?([0-9]|[1-9][0-9]|1[1-7][0-9]|180)(\.\d+)?\s*'
#     r'\]\s*,\s*)*'
#     r'\[\s*'
#     r'-?([0-9]|[1-8][0-9]|90)(\.\d+)?\s*,\s*'
#     r'-?([0-9]|[1-9][0-9]|1[1-7][0-9]|180)(\.\d+)?\s*'
#     r'\]\s*\]\s*\]\s*'
# )
#
#
# def postprocess_geopolygon_re(value) -> str:
#     """
#     In [67]: %timeit postprocess_geopolygon(rus_poly)
#     1.02 ms ± 23.8 µs per loop (mean ± std. dev. of 7 runs, 1000 loops each)
#
#     In [68]: %timeit postprocess_geopolygon_re(rus_poly)
#     738 µs ± 17.3 µs per loop (mean ± std. dev. of 7 runs, 1000 loops each)
#     """
#
#     value = str(value)
#     if geopolygon_re.match(value):
#         return value
#     else:
#         return None
