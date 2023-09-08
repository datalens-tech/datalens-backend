#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

echo 'Cleaning up apt...'

rm -rf /var/lib/apt/lists/
