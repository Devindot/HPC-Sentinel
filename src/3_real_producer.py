import time
import os
import shutil
import random
import pandas as pd

def start_atomic_producer():
    print(">>> [INIT] Initializing HPC-Sentinel Atomic Producer...", flush=True)
    
    # 1. Load Source Data
    data_path = "../data/hpc_dataset_flattened.csv"
    if not os.path.exists(data_path):
        print("CRITICAL ERROR: Source data not found.")
        return

    df = pd.read_csv(data_path)
    if 'hash' not in df.columns: df['hash'] = df.index.astype(str)
    
    # Force Order: Hash First, then Features, then Label
    feature_cols = [f"f{i+1}" for i in range(12)]
    target_cols = ['hash'] + feature_cols + ['label']
    
    # Safety check
    available_cols = [c for c in target_cols if c in df.columns]
    df = df[available_cols]
    
    records = df.to_dict(orient='records')
    print(f">>> [LOAD] Loaded {len(records)} packets.", flush=True)

    # PATHS
    SAFE_ROOT = r"C:\HPC_Sentinel_Live"
    TEMP_DIR = os.path.join(SAFE_ROOT, "temp")
    INPUT_DIR = os.path.join(SAFE_ROOT, "input")

    if os.path.exists(SAFE_ROOT):
        try: shutil.rmtree(SAFE_ROOT)
        except: pass
    
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(INPUT_DIR, exist_ok=True)

    print(f">>> [READY] Live Zone: {INPUT_DIR}")
    print(">>> [START] Broadcasting Live Traffic...")

    packet_id = 0
    try:
        while True:
            packet = random.choice(records)
            real_hash = str(packet.get('hash', 'UNKNOWN'))
            
            filename = f"pkt_{int(time.time())}_{packet_id}.csv"
            temp_path = os.path.join(TEMP_DIR, filename)
            final_path = os.path.join(INPUT_DIR, filename)

            # CRITICAL: header=False ensures pure data transmission
            pd.DataFrame([packet], columns=available_cols).to_csv(temp_path, index=False, header=False)
            
            try:
                os.rename(temp_path, final_path)
                print(f" -> [SENT] Packet #{packet_id} | Hash: {real_hash[:15]}...")
            except: pass

            packet_id += 1
            time.sleep(0.6)

    except KeyboardInterrupt:
        print("\n>>> [STOP] Transmission Ceased.")

if __name__ == "__main__":
    start_atomic_producer()