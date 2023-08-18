#!/usr/bin/env python3
"""
Helper to delete old images from YC.

Currently only filters stdin to stdout.

Usage example:

    yc compute --profile=dlb-preprod --folder-name=build image list --format=json \
        | ./yc_images_cleaner.py \
        | xargs --max-args=5 yc compute --profile=dlb-preprod --folder-name=build image delete

See also:
https://a.yandex-team.ru/arc_vcs/junk/dmifedorov/image_cleaner/__main__.py
"""

import itertools
import re
import json
import os
import sys


def name_to_component_fallback(name, sep='-', cnt=2):
    pieces = name.split(sep, cnt)
    return sep.join(pieces[:cnt])


def name_to_component(name):
    match = re.search(
        (
            r'^([a-z-]+)'
            r'([0-9].*)'
        ),
        name
    )
    if not match:
        return name_to_component_fallback(name)
    return match.group(1)


def sort_key(image):
    image_name = image['name']
    ts = image_name.rsplit('-', 1)[-1]
    if ts.isdigit():
        return int(ts), image_name
    return 0, image_name


def filter_images(data, fresh_count=3, name_prefix='bi-'):
    """ """
    data = sorted(data, key=lambda item: item['name'])
    data = [
        sorted(list(items), key=sort_key)
        for _, items in itertools.groupby(
            data,
            key=lambda item: name_to_component(item['name']))
    ]
    data = [items[:-fresh_count] for items in data]
    data = [item for items in data for item in items]
    if name_prefix:
        data = [item for item in data if item['name'].startswith(name_prefix)]
    return data


def filter_images_cmd():
    # Generally there should be no need to override those.
    fresh_count = int(os.environ.get('YIC_FRESH_COUNT', '3'))
    name_prefix = os.environ.get('YIC_NAME_PREFIX', 'bi-')
    data_full = json.loads(sys.stdin.read())
    data_filtered = filter_images(data_full, fresh_count=fresh_count, name_prefix=name_prefix)
    to_delete = {image['name'] for image in data_filtered}

    # # Verbose list of 'to delete / not to delete'
    data_full.sort(key=lambda image: image['name'])
    for image in data_full:
        image_name = image['name']
        if not image_name.startswith(name_prefix):
            continue
        prefix = ' X' if image_name in to_delete else '  '
        print(prefix, image_name, image['id'], file=sys.stderr)

    # # Short list of 'to delete'
    # print(len(data_filtered), ':', '   '.join(item['name'] for item in data_filtered), file=sys.stderr)

    # stdout data for further piping
    print(' '.join(item['id'] for item in data_filtered))


if __name__ == '__main__':
    filter_images_cmd()
