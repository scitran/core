#!/bin/bash
set -e
set -x

echo "IN BOOTSTRAP ACCOUNTS"

(

# Parse input parameters...
#
# bootstrap account file
bootstrap_user_file=${1:-'/var/scitran/code/api/bootstrap.json.sample'}


# Move to API folder for relative path assumptions later on
#
cd /var/scitran/code/api

# Export PYTHONPATH for python script later on.
#
export PYTHONPATH=.


# Bootstrap Users
./bin/bootstrap.py -i users ${bootstrap_user_file}


)
