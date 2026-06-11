from pprint import pprint

import tomlkit

from bi_ci.run_tests import pkg_to_targets_ref


def test_read_test_data(sample_pkg_toml):
    with open(sample_pkg_toml) as f:
        doc = tomlkit.load(f)
    pprint(doc)


def test_pkg_to_targets_ref(sample_pkg_toml):
    with open(sample_pkg_toml) as f:
        doc = tomlkit.load(f)
    pprint(pkg_to_targets_ref(sample_pkg_toml.parent, doc))
