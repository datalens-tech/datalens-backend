#!/usr/bin/env bash
set -eu
export DEBIAN_FRONTEND=noninteractive

echo 'Installing taskfile tool...'


TMP_DIR="$(mktemp -d)"

base_url="https://github.com/go-task/task/releases/download"
version="v3.29.1"
case $(arch) in
"aarch64"|"arm64")
    arch="arm64"
    checksum="a7688f188e2f21218a27c383e82ebc24d61677c9609c890ebed68a1fb21e3551"
    ;;
*)
    arch="amd64"
    checksum="e411770abf73d5e094100ab7a1c8278f35b591ecadbfd778200b6b2ad1ee340b"
    ;;
esac

download_url="${base_url}/${version}/task_linux_${arch}.deb"
filename="task.deb"

cd "$TMP_DIR"
wget ${download_url} -O ${filename}
sha256sum ${filename}
echo "${checksum} ${filename}" | sha256sum -c -
dpkg -i ${filename}

rm -r "${TMP_DIR}"
