#!/usr/bin/env bash

set -eux

CUR_DIR=`realpath .`

cd ../../../ops/ci

cat pyproject.toml | grep "= {path = \"../../" | cut -d '/' -f3-5  | cut -d'"' -f1  | sort | uniq > $CUR_DIR/all_local_packages.lst
cat docker_image_base_ci/requirements_external.txt    > $CUR_DIR/requirements_a.txt
cat docker_image_base_ci/requirements_conflicting.txt    > $CUR_DIR/requirements_conflicting.txt

cd -
