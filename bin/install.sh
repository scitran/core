#!/bin/bash

(
cd `git rev-parse --show-toplevel`
pip install -r requirements.txt
)
