#!/usr/bin/env python3
"""
Normalize the formatting of the YAML files in the current directory.
"""

from __future__ import annotations

import os
import ruamel.yaml  # parser that can preserve comments


def main():
    yaml = ruamel.yaml.YAML()
    for dir_name, _, filenames in os.walk('.'):
        for filename in filenames:
            if not filename.endswith('.yaml'):
                continue
            filepath = os.path.join(dir_name, filename)
            with open(filepath) as fobj:
                data = yaml.load(fobj)
            with open(filepath, 'w') as fobj:
                yaml.dump(data, fobj)


if __name__ == '__main__':
    main()
