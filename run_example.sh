#!/bin/bash
# Helper script to run PySpark examples with the virtual environment

if [ ! -d "venv" ]; then
    echo "Virtual environment not found. Creating..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
else
    source venv/bin/activate
fi

if [ -z "$1" ]; then
    echo "Usage: ./run_example.sh <path_to_script>"
    echo "Example: ./run_example.sh src/pyspark_examples/accum&broad.py"
    exit 1
fi

python "$1"
