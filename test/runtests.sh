#!/usr/bin/env bash
set -e

(
	# Set cwd
	unset CDPATH
	cd "$( dirname "${BASH_SOURCE[0]}" )"

	../../../live.sh cmd PYTHONPATH=code/api:code/data nosetests -vv --exe code/api/test/$1
)
