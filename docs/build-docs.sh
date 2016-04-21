#!/usr/bin/env bash

set -e

# change to parent dir
unset CDPATH
cd "$( dirname "${BASH_SOURCE[0]}" )/.."

# Generate the rst files from the api package
sphinx-apidoc -o docs/source -f -d 3 api

# Transform those rst files into html docs
sphinx-build -a -Q -b html docs/source docs/build
