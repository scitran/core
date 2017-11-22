#!/usr/bin/env sh

set -eu
unset CDPATH
cd "$( dirname "$0" )/../.."


USAGE="
Usage:
    $0 [OPTION...] [-- TEST_ARGS...]

Build scitran-core image and run tests in a Docker container.

Options:
    -h, --help          Print this help and exit

    -B, --no-build      Skip rebuilding default Docker image
    --image IMAGE       Use custom Docker image
    -- TEST_ARGS        Arguments passed to tests/bin/run-tests-ubuntu.sh

"

main() {
    local DOCKER_IMAGE=
    local TEST_ARGS=
    local MONGO_VERSION=3.2

    while [ $# -gt 0 ]; do
        case "$1" in
            -B|--no-build)
                DOCKER_IMAGE="scitran/core:testing"
                ;;
            --image)
                DOCKER_IMAGE="$2"
                shift
                ;;
            --)
                shift
                TEST_ARGS="$@"
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

    # Docker build
    if [ -z "${DOCKER_IMAGE}" ]; then
        log "Building scitran/core:testing ..."
        docker build -t scitran/core:testing .
    else
        docker tag "$DOCKER_IMAGE" "scitran/core:testing"
    fi

    trap clean_up EXIT

    docker network create core-test

    local SCITRAN_CORE_DRONE_SECRET="secret"

    # Launch core + mongo
    docker run -d \
        --name core-test-service \
        --network core-test \
        --volume $(pwd)/api:/src/core/api \
        --volume $(pwd)/tests:/src/core/tests \
        --env SCITRAN_CORE_DRONE_SECRET=$SCITRAN_CORE_DRONE_SECRET \
        --env SCITRAN_RUNTIME_COVERAGE=true \
        --env SCITRAN_CORE_ACCESS_LOG_ENABLED=true \
        scitran/core:testing

    # Execute tests
    docker run -it \
        --name core-test-runner \
        --network core-test \
        --volume $(pwd)/api:/src/core/api \
        --volume $(pwd)/tests:/src/core/tests \
        --env SCITRAN_CORE_DRONE_SECRET=$SCITRAN_CORE_DRONE_SECRET \
        --env SCITRAN_PERSISTENT_DB_URI=mongodb://core-test-service:27017/scitran \
        --env SCITRAN_PERSISTENT_DB_LOG_URI=mongodb://core-test-service:27017/logs \
        --env SCITRAN_SITE_API_URL=http://core-test-service/api \
        scitran/core:testing \
        /src/core/tests/bin/run-tests-ubuntu.sh \
        $TEST_ARGS
}


clean_up() {
    local TEST_RESULT_CODE=$?
    set +e

    log "INFO: Test return code = $TEST_RESULT_CODE"
    if [ "${TEST_RESULT_CODE}" = "0" ]; then
        # Copy unit test coverage
        docker cp core-test-runner:/src/core/.coverage .coverage.unit-tests

        # Gracefully stop API then copy integration test coverage
        # TODO added exec to dev+mongo.sh and tried (TERM|KILL|INT|QUIT) signals to no avail
        # Somehow api.web.start/save_coverage() (atexit) is NOT triggered
        # docker kill --signal=SIGTERM core-test-service
        # docker cp core-test-service:/src/core/.coverage.integration-tests ./

        # TODO report/combine/htmlize coverage using a test container
    else
        log "INFO: Printing container logs..."
        docker logs core-test-service
        log "ERROR: Test return code = $TEST_RESULT_CODE. Container logs printed above."
    fi

    # Spin down dependencies
    docker rm -f -v core-test-runner
    docker rm -f -v core-test-service
    docker network rm core-test
    exit $TEST_RESULT_CODE
}


log() {
    printf "\n%s\n" "$@" >&2
}


main "$@"
