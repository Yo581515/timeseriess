#!/bin/bash

source venv/bin/activate

export PYTHONPATH="$(pwd)"

set -a
source .env
set +a

echo "Virtual environment activated"
echo "PYTHONPATH=$PYTHONPATH"
echo "Environment variables loaded from .env"