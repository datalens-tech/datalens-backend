#!/bin/sh -x

set -e

for fln in */setup.py; do (
    set -e
    dn="$(dirname "$fln")"
    cd "$dn"
    rm -rf .dist-info *.dist-info
    python setup.py dist_info
    mv *.dist-info .dist-info
    cp .dist-info/METADATA .dist-info/PKG-INFO
); done


run_sed () {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # GNU/Linux
        sed -i "$1" "$2"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        # Mac OSX
        sed -i '' "$1" "$2"
    fi
}

for fln in */.dist-info/METADATA; do (
    run_sed '/@ file:/ d' $fln
); done
# Skipping these because they don't have dist-info in the arcadia build.
run_sed '/Requires-Dist: yandexcloud/ d; /Requires-Dist: ydb/ d' bi_sqlalchemy_yq/.dist-info/METADATA

for fln in */setup.py; do (
    set -e
    dn="$(dirname "$fln")"
    cd "$dn"
    cp .dist-info/METADATA .dist-info/PKG-INFO
); done
