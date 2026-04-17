#!/bin/bash

echo "Starting bot..."
python bot.py &

echo "Starting streamlit..."

streamlit run admin_streamlit.py \
  --server.address 0.0.0.0 \
  --server.port 8000

wait
