#!/bin/bash

if [ -f "`which brew`" ]; then
    echo "homebrew is installed"
else
    echo "Installing homebrew"
    ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    echo "Installed homebrew"
fi

if brew list | grep openssl > /dev/null; then
    echo "openssl is installed"
else
    echo "Installing openssl"
    brew install openssl
    echo "Installed openssl"
fi

if brew list | grep python > /dev/null; then
    echo "python is installed"
else
    echo "Installing python"
    brew install python
    echo "Installed python"
fi

if [ -f "`which virtualenv`" ]; then
    echo "virtualenv is installed"
else
    echo "Installing virtualenv"
    pip install virtualenv
    echo "Installed virtualenv"
fi

if [ -d "runtime" ]; then
    echo "Virtualenv 'runtime' is present"
else
    echo "Creating python virtualenv"
    virtualenv -p `brew --prefix`/bin/python runtime
    echo "Created python virtualenv 'runtime'"
fi

if [ -f "runtime/bin/mongo" ]; then
    echo "Mongo is installed"
else
    echo "Downloading mongo"
    curl https://fastdl.mongodb.org/osx/mongodb-osx-x86_64-3.0.7.tgz | tar xz -C runtime --strip-components 1
    echo "Mongo installed"
fi

if [ -d "persistent/db" ]; then
    echo "persistent/db exists"
else
    echo "Creating persistent/db"
    mkdir -p persistent/db
    echo "Created persistent/db"
fi


echo "Activating virtualenv"
source runtime/bin/activate

echo "Installing requirements"
pip install -r requirements.txt


# Startup mongo
runtime/bin/mongod --dbpath persistent/db --smallfiles &
MONGO_PID=$!

# Bootstrap and run API
PYTHONPATH=. bin/bootstrap.py configure mongodb://localhost/scitran local Local https://localhost:8080/api oauth_client_id
# PYTHONPATH=. python bin/api.wsgi --data_path persistent/data --ssl --insecure --log_level debug --drone_secret scitran_drone --db_uri mongodb://localhost/scitran
PYTHONPATH=. runtime/bin/paster serve dev.ini --reload

# Shutdown mongo on ctrl+C
kill $MONGO_PID
wait $MONGO_PID
