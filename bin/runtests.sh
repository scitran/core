#!/bin/bash

unit_test_path=test/unit_tests/
code_path=api/

(
case "$1-$2" in
  unit-|unit---ci)
    cd `git rev-parse --show-toplevel`
    PYTHONPATH=. py.test --cov=api --cov-report=term-missing $unit_test_path
    ;;
  unit---watch)
    cd `git rev-parse --show-toplevel`
    PYTHONPATH=. ptw $unit_test_path $code_path -- $unit_test_path
    ;;
  integration-|integration---ci|integration---watch)
    echo "Not implemented yet"
    ;;
  *)
    echo "Usage: $0 unit|integration [--ci|--watch]"
    ;;
esac
)
