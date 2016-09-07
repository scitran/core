set -e

unset CDPATH
cd "$( dirname "${BASH_SOURCE[0]}" )/../.."

echo "Checking for files with DOS encoding:"
! find * -path "runtime" -prune -o -path "persistent" -prune -o \
  -type f -exec file {} \; | grep -I "with CRLF line terminators"

echo "Checking for files with windows style newline:"
! find * -path "runtime" -prune -o -path "persistent" -prune -o -type f \
  -exec grep -rI $'\r' {} \+

# Remove __pycache__ directory for issue with __file__ attribute
# Due to running the tests on the host creating bytecode files
# Which have a mismatched __file__ attribute when loaded in docker container
rm -rf test/unit_tests/python/__pycache__

PYTHONPATH="$( pwd )"  py.test --cov=api --cov-append test/unit_tests/python
