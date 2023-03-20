#!/usr/bin/env bash
echo '----> Running redis-server...'
redis-server &
sleep 5
echo '----> Running fastapi servers...'
python master.py
