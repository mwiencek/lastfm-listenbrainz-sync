#!/bin/bash

while true; do
        ./fetch_scrobbles.py
        ./submit_listens.py
        sleep 180
done
