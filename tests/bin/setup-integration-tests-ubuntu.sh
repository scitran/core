#!/usr/bin/env bash
set -eu
unset CDPATH
cd "$( dirname "${BASH_SOURCE[0]}" )/../.."

sudo pip install -U -r "tests/integration_tests/requirements-integration-test.txt"

