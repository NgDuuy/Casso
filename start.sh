#!/bin/bash

echo "Starting bot..."
python bot.py &

echo "Starting streamlit..."

PORT=${PORT:-8501}

streamlit run admin_streamlit.py \
  --server.port $PORT \
  --server.address 0.0.0.0
