#!/usr/bin/env sh

unset CDPATH
cd "$(dirname "$0")"

nginx

mongod &

if [ "$(stat -c %U:%G $SCITRAN_PERSISTENT_DATA_PATH)" != "nobody:nobody" ]; then
    chown nobody:nobody -R $SCITRAN_PERSISTENT_DATA_PATH
fi

./db_upgrade.py upgrade_schema

exec unitd --control "*:8088" --no-daemon --log /dev/stdout
