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

if [ -f "$PERSITENT_DIR/db/mongod.lock" ]; then
    echo "Database exists at $PERSITENT_DIR/db. Not bootstrapping users."
    BOOTSTRAP_USERS=0
else
    echo "Creating database location at $PERSITENT_DIR/db"
    mkdir -p $PERSITENT_DIR/db
    if ! [ -f "bootstrap.json" ]; then
        echo "Cannot bootstrap users. Please create bootstrap.json from bootstrap.json.sample."
        exit 1
    fi
    BOOTSTRAP_USERS=1
fi


echo "Activating Virtualenv"
source $RUNTIME_DIR/bin/activate

echo "Installing Python requirements"
pip install -U -r requirements.txt


# Launch mongod
mongod --dbpath $PERSITENT_DIR/db --smallfiles &
MONGO_PID=$!

# Set python path so scripts can work
export PYTHONPATH=.

# Configure api
bin/bootstrap.py configure mongodb://localhost/scitran local Local https://localhost:8080/api oauth_client_id

# Boostrap users
if [ "$BOOTSTRAP_USERS" -eq "1" ]; then
    bin/bootstrap.py users mongodb://localhost/scitran bootstrap.json
fi

if [ -d "$PERSITENT_DIR/data" ]; then
    echo "Persistence store exists at $PERSITENT_DIR/data. Not bootstrapping data. Remove to re-bootstrap."
else
    echo "Downloading testdata"
    curl https://codeload.github.com/scitran/testdata/tar.gz/master | tar xz -C $PERSITENT_DIR
    echo "Bootstrapping testdata"
    bin/bootstrap.py data --copy mongodb://localhost/scitran $PERSITENT_DIR/testdata-master $PERSITENT_DIR/data
    echo "Bootstrapped testdata"
    rm -rf $PERSITENT_DIR/testdata-master
    echo "Cleaned up downloaded data"
fi

# Serve API with paste
# python bin/api.wsgi --data_path $PERSITENT_DIR/data --ssl --insecure --log_level debug --drone_secret scitran_drone --db_uri mongodb://localhost/scitran

# Serve API with PasteScript
paster serve dev.ini --reload

# Exit out of the python virtualenv
deactivate

# Shutdown mongod on ctrl+C
kill $MONGO_PID
wait $MONGO_PID
