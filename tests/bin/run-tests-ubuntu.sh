#!/usr/bin/env sh

set -eu
unset CDPATH
cd "$( dirname "$0" )/../.."


USAGE="
Usage:
    $0 [OPTION...] [-- PYTEST_ARGS...]

Runs linting and all tests if no options are provided.
Runs subset of tests when using the filtering options.
Displays coverage report if all tests ran and passed.

Assumes running in scitran-core container or that core and all of its
dependencies are installed the same way as in the Dockerfile, and that
    * TODO scitran-core instance is running at...
    * TODO mongodb is runnin at...

Options:
    -h, --help           Print this help and exit

    -l, --lint           Run linting
    -u, --unit           Run unit tests
    -i, --integ          Run integration tests
    -a, --abao           Run abao tests
    -- PYTEST_ARGS       Arguments passed to py.test

"

main() {
    export RUN_ALL=true
    local RUN_LINT=false
    local RUN_UNIT=false
    local RUN_INTEG=false
    local RUN_ABAO=false
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
            -a|--abao)
                RUN_ALL=false
                RUN_ABAO=true
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
        RUN_ABAO=true
    elif ${RUN_LINT} && ${RUN_UNIT} && ${RUN_INTEG} && ${RUN_ABAO}; then
        # All filtering options were used, the same as none
        RUN_ALL=true
    fi

    # Remove __pycache__ directories for issue with __file__ attribute due to
    # running the tests on the host creating bytecode files hich have a
    # mismatched __file__ attribute when loaded in docker container
    rm -rf tests/unit_tests/python/__pycache__
    rm -rf tests/integration_tests/python/__pycache__

    if ${RUN_LINT}; then
        log "Running pylint ..."
        # TODO Enable Refactor and Convention reports
        # TODO Move --disable into rc
        pylint --reports=no --disable=C,R,W0312,W0141,W0110 api

        # log "Running pep8 ..."
        # pep8 --max-line-length=150 --ignore=E402 api
    fi

    if ${RUN_UNIT}; then
        log "Running unit tests ..."
        rm -f .coverage
        py.test --cov=api --cov-report= tests/unit_tests/python $PYTEST_ARGS
    fi

    if ${RUN_INTEG}; then
        log "Running integration tests ..."
        py.test tests/integration_tests/python $PYTEST_ARGS
    fi

    if ${RUN_ABAO}; then
        log "Running abao tests ..."
        # Create resources that Abao relies on
        python tests/integration_tests/abao/load_fixture.py

        local BASEDIR=$(pwd)
        cd raml/schemas/definitions
        abao ../../api.raml "--server=$SCITRAN_SITE_API_URL" "--hookfiles=../../../tests/integration_tests/abao/abao_test_hooks.js"
        cd $BASEDIR
    fi

    if ${RUN_ALL}; then
        log "OVERALL COVERAGE:"
        coverage combine
        coverage report --show-missing
        coverage html
    fi
}


log() {
    printf "\n%s\n" "$@" >&2
}


main "$@"
