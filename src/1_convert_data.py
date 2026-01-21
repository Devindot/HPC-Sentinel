import json
import csv
import os

# --- CONFIGURATION ---
# Pointing to the .txt files found by your debug script
MALWARE_FILE = os.path.join("..", "data", "malware_post_pca_data.txt")
BENIGN_FILE  = os.path.join("..", "data", "benign_post_pca_data.txt")
OUTPUT_FILE  = os.path.join("..", "data", "hpc_dataset_flattened.csv")

def get_feature_count(filepath):
    """Peeks at the file to count how many numbers are in a row."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            # Get the first program's data
            first_key = next(iter(data))
            first_row = data[first_key][0]
            return len(first_row)
    except:
        return 0

def process_file(filepath, label, writer, expected_cols):
    print(f"Reading file: {filepath} ...")
    
    if not os.path.exists(filepath):
        print(f"   [ERROR] File not found: {filepath}")
        return 0

    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            
        count = 0
        for program_hash, time_series in data.items():
            if isinstance(time_series, list):
                for row in time_series:
                    # We accept the row if it matches our detected column count
                    if len(row) == expected_cols:
                        clean_row = [label, program_hash] + row
                        writer.writerow(clean_row)
                        count += 1
        
        print(f"   -> Extracted {count} rows.")
        return count

    except Exception as e:
        print(f"   [Error] {e}")
        return 0

# --- MAIN EXECUTION ---
print("--- STARTING SMART CONVERSION ---")

# 1. Detect how many columns we are dealing with
num_features = get_feature_count(MALWARE_FILE)
if num_features == 0:
    # Fallback if detection fails
    num_features = 12 

print(f"Detected {num_features} features per row.")

# 2. Generate Header (label, hash, f1, f2, ... f12)
header = ["label", "hash"] + [f"f{i+1}" for i in range(num_features)]

try:
    with open(OUTPUT_FILE, 'w', newline='') as f_out:
        writer = csv.writer(f_out)
        writer.writerow(header)
        
        # Process Malware
        m_count = process_file(MALWARE_FILE, 1, writer, num_features)
        
        # Process Benign
        b_count = process_file(BENIGN_FILE, 0, writer, num_features)
        
        total = m_count + b_count
        print("="*30)
        print(f"TOTAL DATASET SIZE: {total} rows")
        print(f"Saved to: {OUTPUT_FILE}")
        print("="*30)

except Exception as e:
    print(f"System Error: {e}")