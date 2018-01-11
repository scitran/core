#!/usr/bin/env sh

set -eu
unset CDPATH
cd "$( dirname "$0" )/../.."


USAGE="
Usage:
    $0 [OPTION...] [-- PYTEST_ARGS...]

Runs linting and all tests if no options are provided.
Runs subset of tests when using the filtering options.

Assumes running in a scitran/core:testing container or that core and all
of its dependencies are installed the same way as in the Dockerfile.

Options:
    -h, --help           Print this help and exit

    -l, --lint           Run linting
    -u, --unit           Run unit tests
    -i, --integ          Run integration tests
    -- PYTEST_ARGS       Arguments passed to py.test

Envvars (required for integration tests):
    SCITRAN_SITE_API_URL            URI to a running core instance (including /api)
    SCITRAN_CORE_DRONE_SECRET       API shared secret
    SCITRAN_PERSISTENT_DB_URI       Mongo URI to the scitran DB
    SCITRAN_PERSISTENT_DB_LOG_URI   Mongo URI to the scitran log DB

"


main() {
    export RUN_ALL=true
    local RUN_LINT=false
    local RUN_UNIT=false
    local RUN_INTEG=false
    local PYTEST_ARGS=

    while [ $# -gt 0 ]; do
        case "$1" in
            -l|--lint)
                RUN_ALL=false
                RUN_LINT=true
                ;;
            -u|--unit)
                RUN_ALL=false
                RUN_UNIT=true
                ;;
            -i|--integ)
                RUN_ALL=false
                RUN_INTEG=true
                ;;
            --)
                shift
                PYTEST_ARGS="$@"
                break
                ;;
            -h|--help)
                printf "$USAGE" >&2
                exit 0
                ;;
            *)
                printf "Invalid argument: $1\n" >&2
                printf "$USAGE" >&2
                exit 1
                ;;
        esac
        shift
    done

    if ${RUN_ALL}; then
        # No filtering options used, run everything by default
        RUN_LINT=true
        RUN_UNIT=true
        RUN_INTEG=true
    elif ${RUN_LINT} && ${RUN_UNIT} && ${RUN_INTEG}; then
        # All filtering options were used, the same as none
        RUN_ALL=true
    fi

    if ${RUN_LINT}; then
        log "Running pylint ..."
        # TODO Enable Refactor and Convention reports
        # TODO Move --disable into rc
        pylint --jobs=4 --reports=no --disable=C,R,W0312,W0141,W0110 api

        # log "Running pep8 ..."
        # pep8 --max-line-length=150 --ignore=E402 api
    fi

    if ${RUN_UNIT}; then
        log "Running unit tests ..."
        rm -f .coverage
        PYTHONDONTWRITEBYTECODE=1 py.test --cov=api --cov-report= tests/unit_tests/python $PYTEST_ARGS
    fi

    if ${RUN_INTEG}; then
        log "Running integration tests ..."
        PYTHONDONTWRITEBYTECODE=1 py.test tests/integration_tests/python $PYTEST_ARGS
    fi
}


log() {
    printf "\n%s\n" "$@" >&2
}


main "$@"
