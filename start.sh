#!/bin/bash

echo "Starting bot..."
python bot.py &

echo "Starting streamlit..."

exec streamlit run admin_streamlit.py \
  --server.port ${PORT:-8000} \
  --server.address 0.0.0.0
