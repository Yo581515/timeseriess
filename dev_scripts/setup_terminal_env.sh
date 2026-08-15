#!/bin/bash

if [ "$1" = "vm" ]; then
    source venv/bin/activate
elif [ "$1" = "local" ]; then
    source venv/Scripts/activate
else
    echo "Usage: source $0 {vm|local}"
    return 1 2>/dev/null || exit 1
fi

export PYTHONPATH="$(pwd)"

set -a
source .env
set +a

echo "Environment variables loaded from .env"