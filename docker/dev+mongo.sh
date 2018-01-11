#!/usr/bin/env sh

mongod &
MONGOD_PID=$!

chown nobody:nobody -R $SCITRAN_PERSISTENT_DATA_PATH

exec unitd --control "*:8080" --no-daemon --log /dev/stdout
