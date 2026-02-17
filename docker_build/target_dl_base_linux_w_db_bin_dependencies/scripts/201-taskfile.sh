#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

echo 'Installing taskfile tool...'


TMP_DIR="$(mktemp -d)"

base_url="https://github.com/go-task/task/releases/download"
version="3.48.0"
case $(arch) in
"aarch64"|"arm64")
    arch="arm64"
    checksum="4aea2e015c0b07e998fed1d12dcb6b050b1c2a1a89e17e2c87a8026e40a2d35c"
    ;;
*)
    arch="amd64"
    checksum="c61513b91cacc2d958cfd7b62b4e67e7e386e43b924fc129ffffcc34c805833a"
    ;;
esac

download_url="${base_url}/v${version}/task_${version}_linux_${arch}.deb"
filename="task.deb"

cd "$TMP_DIR"
wget ${download_url} -O ${filename}
sha256sum ${filename}
echo "${checksum} ${filename}" | sha256sum -c -
dpkg -i ${filename}

rm -r "${TMP_DIR}"
