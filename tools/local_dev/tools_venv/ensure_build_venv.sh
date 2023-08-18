#!/usr/bin/env bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

python3 -m venv ${TOOLS_VENV}
source "${TOOLS_VENV}/bin/activate"

TOOLS_REQS="${DIR}/requirements.txt"

pip install -r "${TOOLS_REQS}"  -i https://pypi.yandex-team.ru/simple > /dev/null
