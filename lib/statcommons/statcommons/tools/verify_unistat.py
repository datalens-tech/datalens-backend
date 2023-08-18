#!/usr/bin/env python2
# coding: utf-8
"""
Convenience copy of
https://bb.yandex-team.ru/projects/SEARCH_INFRA/repos/yasm/browse/scripts/sre/verify_unistat.py

Usage example:

    python verify_unistat.py 'http://127.0.0.1:18882/api/unistat/'
"""

import argparse
import json
import logging
import re
import sys
import urllib2
import six


URLOPEN_TIMEOUT = 1.0

logger = logging.getLogger('verify_unistat')

signal_name_re_raw = r'^([a-zA-Z0-9\.\-/@]+_)+([ad][vehmntx]{3}|summ|hgram|max)$'
signal_name_re = re.compile(signal_name_re_raw)

tag_re_raw = """
^(?:
  (?:ctype|prj|geo)=[a-zA-Z0-9\-.]+
  |
  tier=[a-zA-Z0-9\-.@]+
)$
"""

tag_re = re.compile(tag_re_raw, re.X)


def is_anynumber(value):
    return isinstance(value, six.integer_types + (float,))


class VerificationError(Exception):
    pass


def try_or_die(condition, message):
    if not condition:
        raise VerificationError(message)


def verify_hgram(name, j, bucket):
    try_or_die(isinstance(bucket, list) and len(bucket) == 2,
               "histogram buckets must be lists of length two (bucket "
               "{} of signal '{}' is not)".format(j, name))

    bound, weight = bucket
    try_or_die(is_anynumber(bound) and is_anynumber(weight),
               "both bound and weight of histogram bucket must be "
               "numbers (bucket {} of signal '{}' has bound {} and "
               "weight {})".format(j, name, bound, weight))
    try_or_die(weight >= 0, "bucket weight must be non-negative "
                            "(bucket {} of signal '{}' has negative "
                            "weight {})".format(j, name, weight))
    return bound, weight


def is_diff_agent_suffix(name):
    suffix = name.rsplit('_', 1)[-1]
    return suffix in ["hgram", "summ"] or suffix.startswith('d')


def verify(json_body):
    try_or_die(isinstance(json_body, list),
               "top-level element must be a list, not object")

    for i, item in enumerate(json_body):
        try_or_die(isinstance(item, list), "items of top-level list must be "
                   "lists (item {} is not)".format(i))

        try_or_die(len(item) == 2, "items of top-level list must be lists "
                   "of length 2 (list {} has length {})".format(i, len(item)))

        full_name, value = item

        try_or_die(
            isinstance(full_name, six.string_types + (six.text_type,)),
            "first element in item {} is not a string (it is {})".format(
                i, type(full_name)))

        splitted_name = full_name.split(';')
        name = splitted_name[-1]
        tags = splitted_name[:-1]

        try_or_die(signal_name_re.match(name), "signal name must match '{}' "
                   "(name '{}' of item {} does not)".format(signal_name_re_raw, name, i))

        for t in tags:
            try_or_die(tag_re.match(t),
                       "tag expression '{}', does not match '{}'".format(t, tag_re_raw))

        if is_anynumber(value):
            logger.debug("signal '%s' has a single value", name)
            continue

        try_or_die(isinstance(value, list), "signal values must be either "
                   "numbers or histograms, value {} of signal '{}' "
                   "is neither".format(value, name))

        prev_bound = 0
        contain_buckets = True

        for j, bucket in enumerate(value):

            if is_diff_agent_suffix(name):
                bound, weight = verify_hgram(name, j, bucket)

            else:

                # absolute hgrams may contain plain list of numbers
                if is_anynumber(bucket):
                    contain_buckets = False
                    continue

                # impossible to have numbers and buckets at the same time
                try_or_die(contain_buckets, "histogram must be a list of numbers or "
                                            "a list of buckets, no mixed cases allowed {}".format(name))

                # or it can be a real hgram
                bound, weight = verify_hgram(name, j, bucket)

            if j == 0 and bound == 0:
                # бакит от нуля до нуля
                continue

            try_or_die(bound > prev_bound, "histogram buckets must go from "
                       "lower bounds to higher ones (bucket {} of signal '{}' "
                       "has bound {} while the bound of the previous bucket "
                       "was {})".format(j, name, bound, prev_bound))
            prev_bound = bound
        logger.debug("signal '%s' is a correct histogram", name)


def load_and_verify(url):
    logger.debug("connecting to %s (timeout=%d)", url, URLOPEN_TIMEOUT)
    response = urllib2.urlopen(url, timeout=URLOPEN_TIMEOUT).read()
    logger.debug("got %d bytes of response, trying to decode json",
                 len(response))
    json_body = json.loads(response)
    logger.debug("json parsing was successfull, checking for well-formedness")
    verify(json_body)
    logger.info("checked %d signals from %s, everything seems to be correct",
                len(json_body), url)


def main():
    parser = argparse.ArgumentParser(description='Verify unistat http handle')
    parser.add_argument('url', help='unistat handle endpoint url')
    parser.add_argument('--debug', action='store_true',
                        help='unfold exception tracebacks, be verbose')
    args = parser.parse_args()
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)
    try:
        load_and_verify(args.url)
    except Exception as exc:
        if args.debug:
            raise
        else:
            logger.error(str(exc))
            sys.exit(1)


if __name__ == '__main__':
    main()
