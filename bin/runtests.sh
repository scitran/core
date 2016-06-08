#!/bin/bash

# Convenience script for unit and integration test execution consumed by
# continous integration workflow (travis)
#
# Must return non-zero on any failure.
set -e

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
    # Bootstrap and run integration test.
    #  - always stop and remove docker containers
    #  - always exit non-zero if either bootstrap or integration tests fail.
    #  - only run integration tests on bootstrap success

      docker-compose \
        -f test/docker-compose.yml \
        run \
        --rm \
        bootstrap  && \
      docker-compose \
        -f test/docker-compose.yml \
        run \
        --rm \
        integration-test || \
      exit_code=1
    docker-compose -f test/docker-compose.yml down
    exit $exit_code
    ;;
  integration---watch)
    echo "Not implemented"
    ;;
  *)
    echo "Usage: $0 unit|integration [--ci|--watch]"
    ;;
esac
)
