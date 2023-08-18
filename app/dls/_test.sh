#!/bin/sh -x
# #######
# A convenience script for quickly running the current tests in the current
# python.
# #######
set -e

if [ $DLSCTST_ENV_SETUP ]; then
    python3.7 -m venv .env
    . ./.env/bin/activate
    pip install -r dev_requirements.txt
    pip install -e .
fi


if [ -d htmlcov.prev ]; then rm -r htmlcov.prev; fi
if [ -d htmlcov ]; then mv htmlcov htmlcov.prev; fi
py.test -vvs --cov=dlscore "$@"
python -m coverage html
