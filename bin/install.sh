#!/usr/bin/env bash

set -e

unset CDPATH
cd "$( dirname "${BASH_SOURCE[0]}" )/.."

pip install -r requirements.txt
pip install -r requirements_dev.txt
