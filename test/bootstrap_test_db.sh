#!/usr/bin/env bash
set -e

if [ -z "$1" ]
  then
    echo "Usage ./bootstrap_test_db.sh <site_id>"
    exit 1
fi

(
	# Set cwd
	unset CDPATH
	cd "$( dirname "${BASH_SOURCE[0]}" )"

	../../../live.sh cmd PYTHONPATH=code/api:code/data code/api/bin/bootstrap.py users -f mongodb://localhost:9001/scitran code/api/test/test_bootstrap.json $1
)
