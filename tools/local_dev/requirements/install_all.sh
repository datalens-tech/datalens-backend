#!/usr/bin/env bash

set -Eeuo pipefail

root="${PYTHON_PKG_ROOT:-../../..}"
reqsdir="${BI_REQSDIR:-$root/tools/local_dev/requirements}"


local_packages="$(cat ./all_local_packages.lst | sed '/^#/ d')"

editable_install=()
for pkg in $local_packages; do
    editable_install+=("-e")
    editable_install+=("$root/$pkg[all]")
done


set -x

run_pip_install() {
    # TODO: --use-feature=2020-resolver
    pip install \
        --disable-pip-version-check \
        --index-url https://pypi.yandex-team.ru/simple/ \
        --use-deprecated=legacy-resolver \
        "$@"
}

run_pip_install \
    --upgrade \
    pip==22.0.4 setuptools==53.1.0 wheel==0.36.2


run_pip_install \
    --upgrade \
    -c "$reqsdir/requirements_a.txt" \
    "${editable_install[@]}" \
    "$@"
# Hax: second pass make sure the editable versions are installed.
# Should have worked without it, but, unfortunately, did not.
run_pip_install \
    --no-deps \
    --force-reinstall \
    "${editable_install[@]}"
run_pip_install \
    --no-deps \
    --force-reinstall \
    --index-url https://pypi.yandex-team.ru/simple/ \
    certifi-yandex
