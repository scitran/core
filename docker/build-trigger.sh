#!/usr/bin/env bash

# Triggers an auto-build on Docker Hub for the given source control reference.
#
# Example usage: ./build-trigger Tag 1.0.0 https://registry.hub.docker.com/u/scitran/reaper/trigger/11111111-2222-3333-4444-abcdefabcdef/

set -e

if [ $# -ne 3 ] ; then
  >&2 echo "Usage: $( basename $0 ) <source-control-ref-type> <source-control-ref-value> <trigger-url>"
  exit 1
fi

SOURCE_CONTROL_REF_TYPE="${1}"
SOURCE_CONTROL_REF_NAME="${2}"
TRIGGER_URL="${3}"

if [ -z "${SOURCE_CONTROL_REF_TYPE}" ] ; then
  >&2 echo "Source control reference type not provided. Skipping build trigger."
  exit 1
fi

if [ -z "${SOURCE_CONTROL_REF_NAME}" ] ; then
  >&2 echo "Source control tag name not provided. Skipping build trigger."
  exit 1
fi

TRIGGER_PAYLOAD="{\"source_type\": \"${SOURCE_CONTROL_REF_TYPE}\", \"source_name\": \"${SOURCE_CONTROL_REF_NAME}\"}"
curl -H "Content-Type: application/json" --data "${TRIGGER_PAYLOAD}" -X POST "${TRIGGER_URL}"
>&2 echo
>&2 echo "Docker Hub build for ${SOURCE_CONTROL_REF_TYPE} '${SOURCE_CONTROL_REF_NAME}' triggered."
