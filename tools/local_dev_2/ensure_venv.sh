#!/bin/bash

set -eux

PYTHON_EXECUTABLE=${PYTHON_EXECUTABLE:-python3}

# Check if HOST_VENV_ROOT is set
if [[ -z $HOST_VENV_ROOT ]]; then
  echo "Error: HOST_VENV_ROOT environment variable is not set."
  exit 1
fi

# Check if HOST_VENV_ROOT directory exists
if [[ ! -d $HOST_VENV_ROOT ]]; then
  echo "Creating directory: $HOST_VENV_ROOT"
  mkdir -p "$HOST_VENV_ROOT"
fi

# Check if the directory contains a Python virtual environment
if [[ ! -d $HOST_VENV_ROOT/bin ]]; then
  echo "Creating a new Python virtual environment in $HOST_VENV_ROOT"
  "${PYTHON_EXECUTABLE}" -m venv "$HOST_VENV_ROOT"
fi

source "$HOST_VENV_ROOT/bin/activate"

expected_version="Poetry (version 1.5.0)"

if [[ "${expected_version}" == "$(poetry --version 2>&1)" ]]; then
  echo "Poetry $expected_version is already installed. Skipping installation."
else
  # Install Poetry using pip
  echo "Installing Poetry..."
  pip install --upgrade pip
  pip install poetry==1.5.0
fi

# Install dependencies and scripts
cd datalens_local_dev || exit 2
poetry install --with dev

