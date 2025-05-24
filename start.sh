#!/bin/bash

# Update pip and install dependencies
echo "Updating pip and installing dependencies..."
python -m pip install --upgrade pip
pip install -r requirements.txt

# Start the application
echo "Starting application..."
uvicorn gucci_api:app --host 0.0.0.0 --port ${PORT:-8000} --reload
uvicorn gucci_api:app --host 0.0.0.0 --port ${PORT:-8000}