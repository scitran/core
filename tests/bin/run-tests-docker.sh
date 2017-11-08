#!/usr/bin/env bash
set -eu
unset CDPATH
cd "$( dirname "${BASH_SOURCE[0]}" )/../.."


function usage() {
cat >&2 <<EOF
Build scitran-core image and run tests in a docker container

Usage:
    $0 [OPTION...] [-- TEST_ARGS...]

Options:
    -B, --no-build      Skip docker build
    -h, --help          Print this help and exit
    -- TEST_ARGS        Arguments passed to tests/bin/run-tests-ubuntu.sh

EOF
}


function main() {
    local DOCKER_BUILD=true
    local TEST_ARGS=

    while [[ "$#" > 0 ]]; do
        case "$1" in
            -B|--no-build)    DOCKER_BUILD=false;              ;;
            -h|--help)        usage;                    exit 0 ;;
            --)               TEST_ARGS="${@:2}";       break  ;;
            *) echo "Invalid argument: $1" >&2; usage;  exit 1 ;;
        esac
        shift
    done

    trap clean_up EXIT

    if ${DOCKER_BUILD}; then
        echo "Building scitran-core:run-tests ..."
        docker build -t scitran-core:run-tests .
    fi

    docker network create scitran-core-test-network

    # Launch Mongo instance
    docker run -d \
        --name scitran-core-test-mongo \
        --network scitran-core-test-network \
        mongo

    # Execute tests
    docker run -it \
        --name scitran-core-test-uwsgi \
        --network scitran-core-test-network \
        -e SCITRAN_PERSISTENT_DB_URI=mongodb://scitran-core-test-mongo:27017/scitran \
        -e SCITRAN_PERSISTENT_DB_LOG_URI=mongodb://scitran-core-test-mongo:27017/logs \
        -v $(pwd):/var/scitran/code/api \
        --entrypoint bash \
        scitran-core:run-tests \
        /var/scitran/code/api/tests/bin/run-tests-ubuntu.sh \
        $TEST_ARGS
}


function clean_up() {
    local TEST_RESULT_CODE=$?
    set +e

    # Copy coverage file to host for possible further reporting
    docker cp scitran-core-test-uwsgi:/var/scitran/code/api/.coverage .coverage

    # Spin down dependencies
    docker rm -f -v scitran-core-test-uwsgi
    docker rm -f -v scitran-core-test-mongo
    docker network rm scitran-core-test-network
    exit $TEST_RESULT_CODE
}


main "$@"
