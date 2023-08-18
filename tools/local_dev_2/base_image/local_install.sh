#!/usr/bin/env bash

set -eux

INITIAL_LOCAL_INSTALL_MARK="/venv/_INITIAL_LOCAL_INSTALL_MARK"

if [ -f "$INITIAL_LOCAL_INSTALL_MARK" ]; then
    echo "install done, skipping"
else
    echo "local install"
    source /poetry_migration_venv/bin/activate

    cd /data/ops/ci || exit 1
    # poetry lock --no-update
    poetry export --without-hashes --with=dev --with=ci --format=requirements.txt | grep  'file:///' > /tmp/local_requirements.txt
    poetry export --without-hashes --with=dev --with=ci --format=requirements.txt | grep  -v 'file:///' > /tmp/ext_requirements.txt
    python /make_requirements_editable.py /tmp/local_requirements.txt /tmp/local_editable.txt

    rsync -az /backed_venv/ /venv/

    source /venv/bin/activate
    # should not cause new versions unless there is some package updates in current branch
    pip install --no-deps -r /tmp/ext_requirements.txt

    # slow
    pip install --no-deps -r /tmp/local_editable.txt

    touch "$INITIAL_LOCAL_INSTALL_MARK"
fi

# moving venv onto the volume
# last / is important for rsync!, don't touch!
# rsync -az /backed_venv/ /venv

