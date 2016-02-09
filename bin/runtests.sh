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
  integration---ci)
    PYTHONPATH=. py.test $integration_test_path
    ;;
  integration-)
    PYTHONPATH=. py.test $integration_test_path
    ;;
  integration---watch)
    echo "Not implemented"
    ;;
  *)
    echo "Usage: $0 unit|integration [--ci|--watch]"
    ;;
esac
)
