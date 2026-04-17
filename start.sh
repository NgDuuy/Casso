#!/bin/bash

echo "Starting bot..."
python bot.py &

echo "Starting streamlit..."
streamlit run admin_streamlit.py \
  --server.port 8501 \
  --server.address 0.0.0.0 &

wait