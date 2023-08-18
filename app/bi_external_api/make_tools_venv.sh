#!/usr/bin/env bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
VENV_PATH="${DIR}/.venv"

python3 -m venv "${VENV_PATH}"
source "${VENV_PATH}/bin/activate"

pip install -e .[all] -i https://pypi.yandex-team.ru/simple --use-deprecated=legacy-resolver -c "${DIR}/../../tools/local_dev/requirements/requirements_a.txt"
