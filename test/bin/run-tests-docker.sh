#!/usr/bin/env bash
set -e

unset CDPATH
cd "$( dirname "${BASH_SOURCE[0]}" )/../.."

IMAGE_NAME_SCITRAN_CORE="scitran-core:run-tests"
IMAGE_NAME_MONGO=mongo
CONTAINER_NAME_MONGO=scitran-core-test-mongo
CONTAINER_NAME_SCITRAN_CORE=scitran-core-test-uwsgi


USAGE="
    Run scitran-core tests using docker
\n
    Usage:\n
    \n
    --help: print help and exit\n
    -b, --build-image: Rebuild scitran-core base image\n
    -L, --no-lint: Skip linter\n

"

SCITRAN_RUN_LINT="true"
BUILD_IMAGE="false"

while [ "$#" -gt 0 ]; do
    key="$1"
    case $key in
        --help)
        echo -e $USAGE >&2
        exit 1
        ;;
        -b|--build-image)
        BUILD_IMAGE="true"
        ;;
        -L|--no-lint)
        SCITRAN_RUN_LINT="false"
        ;;
        *)
        echo "Invalid option: $key" >&2
        echo -e $USAGE >&2
        exit 1
        ;;
    esac
    shift
done


clean_up () {
  # Copy coverage file to host for possible further reporting
  docker cp "$CONTAINER_NAME_SCITRAN_CORE":/var/scitran/code/api/.coverage .coverage || true
  # Stop and remove containers
  docker rm -v -f "$CONTAINER_NAME_MONGO"
  docker rm -v -f "$CONTAINER_NAME_SCITRAN_CORE"
}
trap clean_up EXIT

if [[ $( docker images "$IMAGE_NAME_SCITRAN_CORE" | tail -n +2 ) == "" ]]; then
  echo "scitran-core image not found.  Building"
  BUILD_IMAGE="true"
fi

if [ "$BUILD_IMAGE" == "true" ]; then
  docker build -t "$IMAGE_NAME_SCITRAN_CORE" .
fi

# Sub-shell the test steps to make the functionality of the trap execution explicit
(
# Launch Mongo isinstance
docker run --name "$CONTAINER_NAME_MONGO" -d "$IMAGE_NAME_MONGO"

# Execute tests
docker run \
  --name "$CONTAINER_NAME_SCITRAN_CORE"\
  -e "SCITRAN_PERSISTENT_DB_URI=mongodb://$CONTAINER_NAME_MONGO:27017/scitran" \
  -e "SCITRAN_RUN_LINT=$SCITRAN_RUN_LINT" \
  --link "$CONTAINER_NAME_MONGO" \
  -v $(pwd):/var/scitran/code/api \
  --entrypoint bash \
  "$IMAGE_NAME_SCITRAN_CORE" \
    /var/scitran/code/api/test/bin/run-tests-ubuntu.sh
)
