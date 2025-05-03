#!/usr/bin/env python3
import os
import time
import shutil

INCOMING = "/data/incoming"
PROCESSING = "/data/processing"

os.makedirs(PROCESSING, exist_ok=True)

print("Starting file mover service...")

while True:
    try:
        files = [f for f in os.listdir(INCOMING) if f.endswith(".csv")]
        for f in files:
            src = os.path.join(INCOMING, f)
            dst = os.path.join(PROCESSING, f)
            shutil.move(src, dst)
            print(f"Moved file {f} to processing directory")
    except Exception as e:
        print(f"Error moving files: {e}")
    time.sleep(2)
