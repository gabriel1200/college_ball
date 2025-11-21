#!/bin/bash
source venv/bin/bash
# Run live_scrape.py every 15 seconds for 4 minutes (16 runs)
end=$((SECONDS+240))

while [ $SECONDS -lt $end ]; do
    echo "Running live_scrape.py at $(date)"
    python live_scrape2.py
    sleep 10
done

echo "Test complete at $(date)"
