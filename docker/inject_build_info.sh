#!/usr/bin/env sh

set -eu
cat > version.json <<EOF
{
  "branch": "$1",
  "commit": "$2",
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF
cat version.json
