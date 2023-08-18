#!/usr/bin/env bash

set -euxo pipefail

source .env

SRC=$(realpath ../../)
DST=$(realpath $DIR_GH_CHECKOUT_SYNC)
TMP_GIT=$(realpath ./tmp_git)
TARGET_BRANCH=${USER}

# make sure to generate translation files
cd $SRC/lib/bi_formula_ref
make translations
cd -

echo Going to update $DST with a working copy from $SRC

# preserver .git in a tmp dir
mkdir -p $TMP_GIT || true
rm -rf $TMP_GIT/*
cp -rf $DST/.git $TMP_GIT/

# remove old content in the
rm -rf  $DST/
mkdir $DST
mv ./tmp_git/.git $DST/

cd $DST && git switch $TARGET_BRANCH && cd -
rsync -avz --exclude ".terraform/" --exclude ".venv/"  --exclude ".tools_venv" --exclude "mypy/" --exclude ".env/" --exclude "a.yaml" --exclude "ya.make" --exclude ".idea/"  $SRC/ $DST/

cd $DST
git add .
git commit -m "${USER} dev branch, update $(date)"
git push -f
