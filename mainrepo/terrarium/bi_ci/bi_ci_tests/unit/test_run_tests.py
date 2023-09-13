from pprint import pprint

import tomlkit

from bi_ci.run_tests import pkg_to_targets_ref


def test_read_test_data(sample_pkg_toml):
    doc = tomlkit.load(open(sample_pkg_toml))
    pprint(doc)


def test_pkg_to_targets_ref(sample_pkg_toml):
    doc = tomlkit.load(open(sample_pkg_toml))
    pprint(pkg_to_targets_ref(sample_pkg_toml.parent, doc))
