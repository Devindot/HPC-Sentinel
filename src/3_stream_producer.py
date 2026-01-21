import time
import pandas as pd
import shutil
import os
import random

# --- CONFIGURATION ---
SOURCE_DATA = "../data/hpc_dataset_flattened.csv"
DEST_FOLDER = "../stream_stage"

print("--- HPC-SENTINEL TRAFFIC GENERATOR ---")
print(f"Loading source data from {SOURCE_DATA}...")

# Load the full dataset so we can pick random attacks from it
try:
    df = pd.read_csv(SOURCE_DATA)
    print(f"Loaded {len(df)} rows of telemetry.")
except Exception as e:
    print(f"Error loading data: {e}")
    exit()

# Clear the stage first (remove old files)
if os.path.exists(DEST_FOLDER):
    for f in os.listdir(DEST_FOLDER):
        os.remove(os.path.join(DEST_FOLDER, f))
else:
    os.makedirs(DEST_FOLDER)

print("Starting Simulation... (Press Ctrl+C to stop)")
print("Injecting server telemetry every 2 seconds...")

batch_id = 0
try:
    while True:
        # 1. Pick a random chunk of data (5-10 rows)
        sample_size = random.randint(5, 10)
        chunk = df.sample(n=sample_size)
        
        # 2. Generate a filename based on time
        timestamp = int(time.time())
        filename = f"telemetry_{timestamp}_{batch_id}.csv"
        output_path = os.path.join(DEST_FOLDER, filename)
        
        # 3. Write it to the 'stage' folder
        chunk.to_csv(output_path, index=False)
        
        print(f"[{batch_id}] Sent {sample_size} packets -> {filename}")
        
        batch_id += 1
        time.sleep(2) # Wait 2 seconds before next burst

except KeyboardInterrupt:
    print("\nSimulation Stopped.")