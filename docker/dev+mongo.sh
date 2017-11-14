#!/usr/bin/env sh

mongod &
MONGOD_PID=$!

unitd --control "*:8888" --no-daemon --log /dev/stdout
