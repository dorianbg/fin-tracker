#!/bin/zsh

set -e

export POETRY_DOTENV_LOCATION=fintracker/env_vars/prod.env;
export PYTHONPATH="${PYTHONPATH}:fintracker";

/opt/homebrew/bin/poetry run python fintracker/yahoo_prices.py;