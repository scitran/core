#!/bin/bash

(
cd `git rev-parse --show-toplevel`
PYTHONPATH=. py.test --cov=api --cov-report=term-missing test/unit_tests/
)
