#!/usr/bin/env bash

set -eu

unset CDPATH
cd "$( dirname "${BASH_SOURCE[0]}" )/.."

# Add the apt repo for modern node-js, this will run apt-get update
curl -sL https://deb.nodesource.com/setup_8.x | sudo bash -

sudo apt-get install -y \
    build-essential \
    ca-certificates \
    libatlas3-base \
    numactl \
    python-dev \
    libffi-dev \
    libssl-dev \
    libpcre3 \
    libpcre3-dev \
    git \
	nodejs

sudo pip install -U pip

sudo pip install -r requirements.txt
