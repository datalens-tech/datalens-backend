#!/usr/bin/env bash

set -Eeuo pipefail

root="${PYTHON_PKG_ROOT:-../../..}"
reqsdir="${BI_REQSDIR:-$root/tools/local_dev/requirements}"


local_packages="$(cat ./all_local_packages.lst | sed '/^#/ d')"

editable_install=()
for pkg in $local_packages; do
    editable_install+=("-e")
    editable_install+=("$root/$pkg")
done


set -x

# only third party deps here
cat "$reqsdir/requirements_a.txt"
pip install -r "$reqsdir/requirements_a.txt"
pip install -e /data/terrarium/bi_ci

# workaround
pip install --no-deps --ignore-installed -r "$reqsdir/requirements_conflicting.txt"

# local deps
pip install --no-deps --no-build-isolation "${editable_install[@]}"

# local workaround
pip install --no-deps --no-build-isolation -e /data/lib/bi_connector_bigquery

# ensure that certifi yandex installed last
pip install --index-url https://pypi.yandex-team.ru/simple --no-deps --ignore-installed --force-reinstall "$(cat /bitools/requirements/requirements_a.txt | grep certifi-yandex)"

