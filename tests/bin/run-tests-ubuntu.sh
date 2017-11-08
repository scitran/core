#!/usr/bin/env bash
set -eu
unset CDPATH
cd "$( dirname "${BASH_SOURCE[0]}" )/../.."


function usage() {
cat >&2 <<EOF
Run scitran-core tests

Usage:
    $0 [OPTION...]

Options:
    -L, --no-lint           Skip linting
    -U, --no-unit           Skip unit tests
    -I, --no-integ          Skip integration tests
    -A, --no-abao           Skip abao tests
    -R, --no-report         Skip coverage report
    -h, --help              Print this help and exit
    -- PYTEST_ARGS          Arguments passed to py.test

Envvars:
    SCITRAN_PERSISTENT_DB_PORT    (9001)
    SCITRAN_PERSISTENT_DB_URI     (mongodb://localhost:9001/scitran)
    SCITRAN_PERSISTENT_DB_LOG_URI (mongodb://localhost:9001/logs)

Assumes mongo db instance is accessible at localhost, unless
SCITRAN_PERSISTENT_DB_URI or SCITRAN_PERSISTENT_DB_LOG_URI specify otherwise.

EOF
}


function main() {
    local RUN_LINT=true
    local RUN_UNIT=true
    local RUN_INTEG=true
    local RUN_ABAO=true
    local PYTEST_ARGS=

    export RUN_REPORT=true

    while [[ "$#" > 0 ]]; do
        case "$1" in
            -L|--no-lint)     RUN_LINT=false   ;;
            -U|--no-unit)     RUN_UNIT=false   ;;
            -I|--no-integ)    RUN_INTEG=false  ;;
            -A|--no-abao)     RUN_ABAO=false   ;;
            -R|--no-report)   RUN_REPORT=false ;;
            -h|--help)        usage;                   exit 0 ;;
            --)               PYTEST_ARGS="${@:2}";    break  ;;
            *) echo "Invalid argument: $1" >&2; usage; exit 1 ;;
        esac
        shift
    done

    if ! (${RUN_LINT} && ${RUN_UNIT} && ${RUN_INTEG} && ${RUN_ABAO}); then
        # Skip coverage report if any tests are skipped
        RUN_REPORT=false
    fi

    trap clean_up EXIT

    # Remove __pycache__ directories for issue with __file__ attribute due to
    # running the tests on the host creating bytecode files hich have a
    # mismatched __file__ attribute when loaded in docker container
    rm -rf tests/unit_tests/python/__pycache__
    rm -rf tests/integration_tests/python/__pycache__

    export PYTHONPATH="$(pwd)"
    export SCITRAN_SITE_API_URL="http://localhost:8081/api"
    export SCITRAN_PERSISTENT_DB_PORT=${SCITRAN_PERSISTENT_DB_PORT:-"9001"}
    export SCITRAN_PERSISTENT_DB_URI=${SCITRAN_PERSISTENT_DB_URI:-"mongodb://localhost:$SCITRAN_PERSISTENT_DB_PORT/scitran"}
    export SCITRAN_PERSISTENT_DB_LOG_URI=${SCITRAN_PERSISTENT_DB_LOG_URI:-"mongodb://localhost:$SCITRAN_PERSISTENT_DB_PORT/logs"}
    export SCITRAN_PERSISTENT_PATH=`mktemp -d`
    export SCITRAN_PERSISTENT_DATA_PATH="$SCITRAN_PERSISTENT_PATH/data"
    export SCITRAN_CORE_DRONE_SECRET=${SCITRAN_CORE_DRONE_SECRET:-$( openssl rand -base64 32 )}

    if ${RUN_LINT}; then
        echo "Running pylint ..."
        # TODO Enable Refactor and Convention reports
        # TODO Move --disable into rc
        pylint --reports=no --disable=C,R,W0312,W0141,W0110 api

        # echo "Running pep8 ..."
        # pep8 --max-line-length=150 --ignore=E402 api
    fi

    if ${RUN_UNIT}; then
        echo "Running unit tests ..."
        rm -f .coverage
        py.test --cov=api --cov-report= tests/unit_tests/python $PYTEST_ARGS
    fi

    if ${RUN_INTEG} || ${RUN_ABAO}; then
        echo "Spinning up dependencies ..."
        uwsgi --http "localhost:8081" --master --http-keepalive \
            --so-keepalive --add-header "Connection: Keep-Alive" \
            --processes 1 --threads 1 \
            --enable-threads \
            --wsgi-file bin/api.wsgi \
            --die-on-term \
            --logformat '%(addr) - %(user) [%(ltime)] "%(method) %(uri) %(proto)" %(status) %(size) "%(referer)" "%(uagent)" request_id=%(request_id)' \
            --env "SCITRAN_PERSISTENT_DB_URI=$SCITRAN_PERSISTENT_DB_URI" \
            --env "SCITRAN_PERSISTENT_DB_LOG_URI=$SCITRAN_PERSISTENT_DB_LOG_URI" \
            --env "SCITRAN_PERSISTENT_PATH=$SCITRAN_PERSISTENT_PATH" \
            --env "SCITRAN_PERSISTENT_DATA_PATH=$SCITRAN_PERSISTENT_DATA_PATH" \
            --env "SCITRAN_CORE_DRONE_SECRET=$SCITRAN_CORE_DRONE_SECRET" \
            --env "SCITRAN_RUNTIME_COVERAGE=true" \
            --env "SCITRAN_CORE_ACCESS_LOG_ENABLED=true" &
        export API_PID=$!

        echo "Connecting to API"
        until $(curl --output /dev/null --silent --head --fail "$SCITRAN_SITE_API_URL"); do
            printf '.'
            sleep 1
        done
    fi

    if ${RUN_INTEG}; then
        echo "Running integration tests ..."
        py.test tests/integration_tests/python $PYTEST_ARGS
    fi

    if ${RUN_ABAO}; then
        echo "Running abao tests ..."
        # Create resources that Abao relies on
        python tests/integration_tests/abao/load_fixture.py

        # If no VIRTUAL_ENV, make sure /usr/local/bin is in the path
        if [[ -z "${VIRTUAL_ENV:-}" ]]; then
            PATH="/usr/local/bin:$PATH"
            npm install tests/integration_tests
        else
            npm install --global tests/integration_tests
        fi

        PATH="$(npm bin):$PATH"

        # Allow us to require modules from package.json,
        # since abao_test_hooks.js is not being called from the package directory
        integration_test_node_modules="$(pwd)/node_modules/scitran-core-integration-tests/node_modules"

        # Have to change into definitions directory to resolve
        # relative $ref's in the jsonschema's
        pushd raml/schemas/definitions
        NODE_PATH="$integration_test_node_modules" abao ../../api.raml "--server=$SCITRAN_SITE_API_URL" "--hookfiles=../../../tests/integration_tests/abao/abao_test_hooks.js"
        popd
    fi
}


function clean_up () {
    local TEST_RESULT_CODE=$?
    set +e

    echo
    echo "Test return code = $TEST_RESULT_CODE"

    if [[ -n "${API_PID:-}" ]]; then
        # Killing uwsgi
        kill $API_PID
        wait 2> /dev/null
    fi

    if ${RUN_REPORT} && [[ "${TEST_RESULT_CODE}" == "0" ]]; then
        echo
        echo "UNIT TEST COVERAGE:"
        coverage report --skip-covered
        echo
        echo "OVERALL COVERAGE:"
        coverage combine
        coverage report --show-missing
        coverage html
    else
        echo "Some tests were skipped or failed, skipping coverage report"
    fi

    exit $TEST_RESULT_CODE
}


main "$@"
