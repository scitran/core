#!/usr/bin/env bash

set -e

RUNTIME_DIR="./runtime"
PERSITENT_DIR="./persistent"

if [ "$#" -ge 1 ]; then
    PERSITENT_DIR="$1"
fi
if [ "$#" -eq 2 ]; then
    RUNTIME_DIR="$2"
fi
if [ "$#" -gt 2 ]; then
    echo "Usage: $0 persistent runtime"
    exit 1
fi

if ! [ -f "bootstrap.json" ]; then
    echo "Please create bootstrap.json from bootstrap.json.sample"
    exit 1
fi

if [ -f "`which brew`" ]; then
    echo "Homebrew is installed"
else
    echo "Installing Homebrew"
    ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    echo "Installed Homebrew"
fi

if brew list | grep -q openssl; then
    echo "OpenSSL is installed"
else
    echo "Installing OpenSSL"
    brew install openssl
    echo "Installed OpenSSL"
fi

if brew list | grep -q python; then
    echo "Python is installed"
else
    echo "Installing Python"
    brew install python
    echo "Installed Python"
fi

if [ -f "`which virtualenv`" ]; then
    echo "Virtualenv is installed"
else
    echo "Installing Virtualenv"
    pip install virtualenv
    echo "Installed Virtualenv"
fi

if [ -d "$RUNTIME_DIR" ]; then
    echo "Virtualenv exists present at $RUNTIME_DIR"
else
    echo "Creating 'scitran' Virtualenv at $RUNTIME_DIR"
    virtualenv -p `brew --prefix`/bin/python --prompt="(scitran)" $RUNTIME_DIR
    echo "Created 'scitran' Virtualenv at $RUNTIME_DIR"
fi

if [ -f "$RUNTIME_DIR/bin/mongod" ]; then
    echo "MongoDB is installed"
else
    echo "Installing MongoDB"
    curl https://fastdl.mongodb.org/osx/mongodb-osx-x86_64-3.0.7.tgz | tar xz -C $RUNTIME_DIR --strip-components 1
    echo "MongoDB installed"
fi

if [ -d "$PERSITENT_DIR/db" ]; then
    echo "Persistence store exists at $PERSITENT_DIR/db"
else
    echo "Creating persistence store exists at $PERSITENT_DIR/db"
    mkdir -p $PERSITENT_DIR/db
fi


echo "Activating Virtualenv"
source $RUNTIME_DIR/bin/activate

echo "Installing Python requirements"
pip install -U -r requirements.txt


# Launch mongod
mongod --dbpath $PERSITENT_DIR/db --smallfiles &
MONGO_PID=$!

# Bootstrap and run API
export PYTHONPATH=.
bin/bootstrap.py configure mongodb://localhost/scitran local Local https://localhost:8080/api oauth_client_id
#python bin/api.wsgi --data_path $PERSITENT_DIR/data --ssl --insecure --log_level debug --drone_secret scitran_drone --db_uri mongodb://localhost/scitran
paster serve dev.ini --reload

# Shutdown mongod on ctrl+C
kill $MONGO_PID
wait $MONGO_PID
