#!/usr/bin/env bash
set -e

unset CDPATH
cd "$( dirname "${BASH_SOURCE[0]}" )/../.."

set -a
# Use port 9003 to hopefully avoid conflicts
SCITRAN_RUNTIME_PATH=${SCITRAN_RUNTIME_PATH:-"$( pwd )/runtime"}
SCITRAN_PERSISTENT_DB_PORT=9003
SCITRAN_PERSISTENT_DB_URI="mongodb://localhost:$SCITRAN_PERSISTENT_DB_PORT/integration-tests"
SCITRAN_PERSISTENT_DB_LOG_URI=${SCITRAN_PERSISTENT_DB_LOG_URI:-"mongodb://localhost:$SCITRAN_PERSISTENT_DB_PORT/logs"}
SCITRAN_PERSISTENT_PATH="$( mktemp -d )"
SCITRAN_CORE_DRONE_SECRET=${SCITRAN_CORE_DRONE_SECRET:-$( openssl rand -base64 32 )}

clean_up () {
  kill $API_PID || true
  wait 2> /dev/null
  rm -rf "$SCITRAN_PERSISTENT_PATH"
  # Report on unit tests and integration tests separately
  coverage report -m
  rm .coverage
  coverage combine
  coverage report -m
}

trap clean_up EXIT

./bin/install-dev-osx.sh

# Note this will fail with "unbound variable" errors if "set -u" is enabled
. "$SCITRAN_RUNTIME_PATH/bin/activate"

./test/bin/lint.sh api

./test/bin/run-unit-tests.sh

SCITRAN_RUNTIME_PORT=8081 \
    SCITRAN_CORE_DRONE_SECRET="$SCITRAN_CORE_DRONE_SECRET" \
    SCITRAN_RUNTIME_COVERAGE="true" \
    ./bin/run-dev-osx.sh -T -U -I &
API_PID=$!

./test/bin/run-integration-tests.sh \
    "http://localhost:8081/api" \
    "$SCITRAN_PERSISTENT_DB_URI" \
    "$SCITRAN_PERSISTENT_DB_LOG_URI" \
    "$SCITRAN_CORE_DRONE_SECRET"
