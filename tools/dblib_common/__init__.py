from __future__ import annotations

import os
import json

from library.python.testing import recipe


def add_to_path(prev, new, position=None):
    paths = prev.split(':') if prev else []
    if new in paths:
        return prev
    if position is None:
        paths.append(new)
    else:
        paths.insert(position, new)
    return ':'.join(paths)


def get_env(key):
    try:
        fobj_cm = open(recipe.ya.env_file)
    except FileNotFoundError:
        return None
    else:
        with fobj_cm as fobj:
            for line in fobj:
                line_data = json.loads(line)
                value = line_data.get(key)
                if value is not None:
                    return value
    return None


def add_to_ldlp(name, path, logger, key='LD_LIBRARY_PATH'):
    path = os.path.realpath(path)
    logger.debug('%s: pid=%r, path=%r', name, os.getpid(), path)
    logger.debug('  ... files=%r', os.listdir(path))

    prev_val = get_env(key)
    prev_val = prev_val or os.environ.get(key)
    new_val = add_to_path(prev=prev_val, new=path)
    logger.debug('  ... %s=%s', key, new_val)
    recipe.set_env(key, new_val)
    return new_val
