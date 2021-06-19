#!/usr/bin/env bash

npm i

python -m venv .venv
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt


