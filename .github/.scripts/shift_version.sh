#!/usr/bin/env bash
set -eo pipefail

version=$1
version_shift=$2

if [[ -z $version ]]; then
    echo "ERROR: No version provided, exiting..."
    exit 1;
fi

if [[ -z $version_shift ]]; then
    echo "ERROR: No version shift provided, exiting..."
    exit 1;
fi

if [[ ! $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "ERROR: Version must be in format X.Y.Z, found $version, exiting..."
    exit 1;
fi

major=$(echo "$version" | cut -d. -f1)
minor=$(echo "$version" | cut -d. -f2)
patch=$(echo "$version" | cut -d. -f3)

case "$version_shift" in
    "major")
        major=$((major + 1))
        minor=0
        patch=0
        ;;
    "minor")
        minor=$((minor + 1))
        patch=0
        ;;
    "patch")
        patch=$((patch + 1))
        ;;
    *)
        echo "ERROR: Version shift must be one of [major, minor, patch], found $version_shift, exiting..."
        exit 1
        ;;
esac

echo "${major}.${minor}.${patch}"
