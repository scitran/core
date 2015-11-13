#!/bin/sh

../../../live.sh cmd PYTHONPATH=code/api:code/data nosetests -vv --exe --collect-only code/api/test/$1