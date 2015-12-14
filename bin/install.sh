#!/bin/bash

(
cd "$( dirname "${BASH_SOURCE[0]}" )/.."
pip install -r requirements.txt
pip install -r dev.requirements.txt
)
