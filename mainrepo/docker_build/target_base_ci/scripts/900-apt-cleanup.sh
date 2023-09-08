#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

echo 'Cleaning up apt...'

apt-get clean
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*