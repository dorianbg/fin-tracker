#!/bin/zsh

set -e

export POETRY_DOTENV_LOCATION=.env;
export PYTHONPATH="${PYTHONPATH}:fintracker";

/opt/homebrew/bin/poetry run python fintracker/executor.py;