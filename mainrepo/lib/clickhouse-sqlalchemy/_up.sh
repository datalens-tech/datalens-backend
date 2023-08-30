#!/bin/sh -x

set -Eeu

# git clean -d -x -f

if git clean -d -x -n | grep -q .; then
    echo "Unclean repo."
fi

python3 -V
twine --version

ver="$(python3 -c '
import sys
import ast

new_version = sys.argv[1]
filename = "./clickhouse_sqlalchemy/__init__.py"
prefix = "VERSION = "
new_version_s = None

def process_line(line):
    global new_version_s
    if not line.startswith(prefix):
        return line
    line = line[len(prefix):]
    if new_version:
        new_version_tuple = tuple(int(item) for item in new_version.strip().split("."))
    else:
        version = ast.literal_eval(line.strip())
        new_version_tuple = version[:-1] + (version[-1] + 1,)
    new_version_s = ".".join(str(item) for item in new_version_tuple)
    return prefix + repr(new_version_tuple) + "\n"

data = "".join(
    process_line(line)
    for line in open(filename))
assert new_version_s, data
with open(filename, "w") as fobj:
    fobj.write(data)
print(new_version_s)
' \
  "${EXPLICIT_NEW_VERSION:-}"
)"

git commit -am "$ver"
git tag "$ver"
git push
git push --tags
python3 ./setup.py sdist bdist_wheel
twine upload --repository yandex dist/*
