#!/usr/bin/env bash

# Triggers an auto-build on dockerhub for the given source control reference.
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
  >&2 echo "INFO: Source control reference type not provided, skipping build trigger."
  exit 0
fi

if [ -z "${SOURCE_CONTROL_REF_NAME}" ] ; then
  >&2 echo "INFO: Source control tag name not provided, skipping build trigger."
  exit 0
fi


# Skip trigger unless branch is master, or for any tag.
EXEC_BUILD_TRIGGER=false

if [ "${SOURCE_CONTROL_REF_TYPE}" == "Branch" ] && [ "${SOURCE_CONTROL_REF_NAME}" == "master" ] ; then
  EXEC_BUILD_TRIGGER=true
fi

if [ "${SOURCE_CONTROL_REF_TYPE}" == "Tag" ] ; then
  EXEC_BUILD_TRIGGER=true
fi

if ! $EXEC_BUILD_TRIGGER ; then
  >&2 echo "INFO: Source control ${SOURCE_CONTROL_REF_TYPE}: '${SOURCE_CONTROL_REF_NAME}' is not enabled for the build trigger."
  exit 0
fi

# Trigger the dockerhub build
TRIGGER_PAYLOAD="{\"source_type\": \"${SOURCE_CONTROL_REF_TYPE}\", \"source_name\": \"${SOURCE_CONTROL_REF_NAME}\"}"
curl -H "Content-Type: application/json" --data "${TRIGGER_PAYLOAD}" -X POST "${TRIGGER_URL}"
>&2 echo
>&2 echo "INFO: A dockerhub build for ${SOURCE_CONTROL_REF_TYPE}: '${SOURCE_CONTROL_REF_NAME}' has been triggered."
