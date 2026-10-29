#!/bin/sh
if [ ! -f /data/config/scales.json ]; then
    echo "Seeding config..."
    cp /app/scales.json /data/config/scales.json
fi
python broker.py --config /data/config/scales.json