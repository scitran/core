#!/bin/bash

unit_test_path=test/unit_tests/
integration_test_path=test/integration_tests/
code_path=api/

cd "$( dirname "${BASH_SOURCE[0]}" )/.."

(
case "$1-$2" in
  unit-)
    PYTHONPATH=. py.test $unit_test_path
    ;;
  unit---ci)
    PYTHONPATH=. py.test --cov=api --cov-report=term-missing $unit_test_path
    ;;
  unit---watch)
    PYTHONPATH=. ptw $unit_test_path $code_path --poll -- $unit_test_path
    ;;
  integration---ci|integration-)
    docker-compose \
        -f test/docker-compose.yml \
        run \
        --rm \
        -e PYTHONPATH=/var/scitran/code/api \
        --entrypoint /var/scitran/code/api/bin/bootstrap.py \
        scitran-core users /var/scitran/test-config/test_bootstrap.json && \
      docker-compose -f test/docker-compose.yml run --rm integration-test
    docker-compose -f test/docker-compose.yml down
    ;;
  integration---watch)
    echo "Not implemented"
    ;;
  *)
    echo "Usage: $0 unit|integration [--ci|--watch]"
    ;;
esac
)

#
# Coverage not necessary
# Running wsgi container of some kind - use docker/Dockerfile
# Expose on random port - don't care
# Run mongodb container - use open source one with osx workaround...
# Create runtime environment in third container
#    Use uwsgi container or use super small python env one?
# Run py.test there, connect to wsgi and mongodb
#    How does wsgi container connect to mongodb?
#
#
#
#
# (api/)$ docker build -t scitran-core -f docker/Dockerfile .
# (api/)$ docker run --rm --name test-mongo mongo &
# (api/)$ docker run -e "SCITRAN_PERSISTENT_DB_URI=mongodb://test-mongo:27017/scitran" --link test-mongo --rm -v $(pwd)/test/test_bootstrap.json:/accounts.json scitran-core /var/scitran/code/api/bin/bootstrap.py users /accounts.json
# (api/)$ docker run --rm --name scitran-core -e "SCITRAN_PERSISTENT_DB_URI=mongodb://test-mongo:27017/scitran" -e "SCITRAN_CORE_INSECURE=true" -v $(pwd)/persistent/data:/var/scitran/data -v $(pwd):/var/scitran/code/api --link test-mongo scitran-core uwsgi --ini /var/scitran/config/uwsgi-config.ini --http 0.0.0.0:8080 --python-autoreload 1 &
# (test/)$ docker build -t integration-test .
# (test/integration_tests)$ docker run --link test-mongo --link scitran-core -v $(pwd):/usr/src/tests integration-test test_collection.py
#
#
# get to a prompt
# docker run --link test-mongo --link scitran-core -v $(pwd):/usr/src/tests -it --entrypoint=/bin/bash integration-test
