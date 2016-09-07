#!/usr/bin/env bash
set -e

unset CDPATH
cd "$( dirname "${BASH_SOURCE[0]}" )/../.."

IMAGE_NAME_SCITRAN_CORE=scitran-core
IMAGE_NAME_MONGO=mongo
CONTAINER_NAME_MONGO=some-mongo

clean_up () {
  # Stop and remove mongo container
  docker rm -v -f "$CONTAINER_NAME_MONGO"
}
trap clean_up EXIT


# Sub-shell the test steps to make the functionality of the trap execution explicit
(
# Build the docker image
docker build -t "$IMAGE_NAME_SCITRAN_CORE" .

# Launch Mongo isinstance
docker run --name "$CONTAINER_NAME_MONGO" -d "$IMAGE_NAME_MONGO"

# Execute tests
docker run \
  --rm \
  --name scitran-core-tester \
  -e "SCITRAN_PERSISTENT_DB_URI=mongodb://$CONTAINER_NAME_MONGO:27017/scitran" \
  --link "$CONTAINER_NAME_MONGO" \
  -v $(pwd):/var/scitran/code/api \
  --entrypoint bash \
  "$IMAGE_NAME_SCITRAN_CORE" \
    /var/scitran/code/api/test/bin/run-tests-ubuntu.sh

)
