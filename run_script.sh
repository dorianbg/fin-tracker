#!/bin/zsh

set -e

cd "$(dirname "$0")"

export POETRY_DOTENV_LOCATION=.env;
export PYTHONPATH="${PYTHONPATH}:fintracker";

poetry run python fintracker/executor.py # --rewrite_all --skip_backup;