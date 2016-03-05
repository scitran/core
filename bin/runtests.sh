#!/bin/bash
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
      docker-compose \
        -f test/docker-compose.yml \
        run \
        --rm \
        -e PYTHONPATH=/var/scitran/code/api \
        --entrypoint /var/scitran/code/api/bin/bootstrap.py \
        bootstrap users /var/scitran/test-config/test_bootstrap.json && \
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
