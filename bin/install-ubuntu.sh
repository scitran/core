#!/usr/bin/env bash

set -eu

unset CDPATH
cd "$( dirname "${BASH_SOURCE[0]}" )/.."

sudo apt-get update
sudo apt-get install -y \
    build-essential \
    ca-certificates \
    curl \
    libatlas3-base \
    numactl \
    python-dev \
    libffi-dev \
    libssl-dev \
    libpcre3 \
    libpcre3-dev \
    git

sudo pip install -U pip

sudo pip install -r requirements.txt
