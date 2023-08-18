#!/usr/bin/env bash
set -eo pipefail

version="v2-$(date +%Y-%m-%d-%H-%M)"
echo "new version is $version"

docker build -f ./docker/Dockerfile -t registry.yandex.net/data-ui/cloud-builder:$version .
docker push registry.yandex.net/data-ui/cloud-builder:$version

oldVersion=$(cat .version)
echo $version > .version;

README=$(sed -e "s/$oldVersion/$version/g" README.md)
echo "$README">README.md

git add .version README.md
git commit -m "new version ${version}"
git push --force
