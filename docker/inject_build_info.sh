#!/bin/bash
(

set -e

# Set cwd
unset CDPATH
cd "$( dirname "${BASH_SOURCE[0]}" )"

# Dump the build info into version.json so it can be displayed in the footer
# of the site pages.

#  {
#    "commit": "5683785e8cd6efdfd794a79828b2cccd2424ed21",
#    "timestamp": "January 12, 2016 at 2:46:23 PM CST",
#    "branch": "ng-constant"
#  }


  BRANCH_NAME=${1}
  COMMIT_HASH=${2}
  BUILD_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  echo "{
  \"commit\": \"${COMMIT_HASH}\",
  \"timestamp\": \"${BUILD_TIMESTAMP}\",
  \"branch\": \"${BRANCH_NAME}\"
}" > version.json

cat version.json

)
