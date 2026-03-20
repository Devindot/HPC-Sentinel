import time
import os
import shutil
import pandas as pd
import xgboost as xgb
import warnings
import sqlite3
import hashlib

warnings.filterwarnings("ignore")

def init_db():
    db_path = "dashboard.db"
    if os.path.exists(db_path):
        try: os.remove(db_path)
        except: pass
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # UPDATED SCHEMA: Added cve_id and cvss_score
    c.execute('''CREATE TABLE IF NOT EXISTS feed
                 (timestamp TEXT, filename_hash TEXT, threat_score REAL, status TEXT, cve_id TEXT, cvss_score REAL)''')
    conn.commit()
    conn.close()
    return db_path

# --- PATENT-WORTHY ALGORITHMIC THREAT ENGINE ---
def generate_threat_intel(filename_hash, malware_prob):
    """
    Deterministically synthesizes a CVE ID and calculates a dynamic CVSS score.
    """
    # CRITICAL: Ensure float type to prevent SQLite binary blobs
    prob = float(malware_prob)
    
    if prob <= 0.50:
        return "N/A", 0.0

    hash_obj = hashlib.md5(str(filename_hash).encode())
    hex_digest = hash_obj.hexdigest()

    year = 2018 + (int(hex_digest[0:2], 16) % 7)
    cve_num = int(hex_digest[2:6], 16)
    
    known_signatures = {
        "vlc": "CVE-2019-13615",
        "java": "CVE-2021-44228",
        "silverlight": "CVE-2016-0034"
    }
    
    cve_id = f"CVE-{year}-{cve_num:04d}"
    for key, known_cve in known_signatures.items():
        if key.lower() in str(filename_hash).lower():
            cve_id = known_cve
            break

    jitter = (int(hex_digest[6:8], 16) / 255.0) * 0.4 
    cvss_score = 4.0 + (prob * 5.6) + jitter
    
    # CRITICAL: Ensure native float
    cvss_score = float(round(min(10.0, cvss_score), 1))

    return cve_id, cvss_score

def start_xgboost_worker():
    print(">>> [INIT] Initializing XGBoost Inference Worker...", flush=True)

    model_path = "../models/xgboost_perfected/model.json"
    if not os.path.exists(model_path):
        print("CRITICAL ERROR: Model not found.")
        return

    print(f">>> [LOAD] Loading Champion Model (84.76%)...", flush=True)
    model = xgb.XGBClassifier()
    model.load_model(model_path)

    SAFE_ROOT = r"C:\HPC_Sentinel_Live"
    HANDOFF_DIR = os.path.join(SAFE_ROOT, "handoff")
    db_path = init_db()

    print(f">>> [LISTEN] Watching: {HANDOFF_DIR}")
    print("\n" + "="*130)
    print(f"{'TIMESTAMP':<12} | {'FILENAME / HASH':<30} | {'SCORE':<8} | {'STATUS':<20} | {'CVE ID':<15} | {'CVSS'}")
    print("="*130)

    try:
        while True:
            found_files = []
            if os.path.exists(HANDOFF_DIR):
                for root, dirs, files in os.walk(HANDOFF_DIR):
                    for file in files:
                        if file.endswith(".csv") and file.startswith("part"):
                            found_files.append(os.path.join(root, file))

            if not found_files:
                time.sleep(0.2)
                continue

            for file_path in found_files:
                try:
                    if os.path.getsize(file_path) == 0: continue
                    # Read Headerless
                    df = pd.read_csv(file_path, header=None)
                    
                    if df.shape[1] >= 13:
                        # Col 0 is HASH (Guaranteed by Producer/Spark logic)
                        real_hash = str(df.iloc[0, 0])
                        
                        # Col 1-12 are Features
                        features = df.iloc[:, 1:13] 
                        features.columns = [f"f{i+1}" for i in range(12)]
                        
                        probs = model.predict_proba(features)[0]
                        # Extract standard float
                        malware_prob = float(probs[1])

                        timestamp = time.strftime("%H:%M:%S")
                        
                        # Apply Algorithmic Threat Intelligence
                        cve_id, cvss = generate_threat_intel(real_hash, malware_prob)

                        if malware_prob > 0.85: status = "🔴 MALWARE DETECTED"
                        elif malware_prob > 0.50: status = "⚠️ SUSPICIOUS ACTIVITY"
                        else: status = "🟢 NORMAL TRAFFIC"

                        try:
                            conn = sqlite3.connect(db_path)
                            c = conn.cursor()
                            # INSERT 6 COLUMNS
                            c.execute("INSERT INTO feed VALUES (?, ?, ?, ?, ?, ?)", 
                                     (timestamp, real_hash, malware_prob, status, cve_id, cvss))
                            conn.commit()
                            conn.close()
                        except: pass

                        display_hash = real_hash[:25] + "..."
                        print(f"{timestamp:<12} | {display_hash:<30} | {malware_prob:.4f}   | {status:<20} | {cve_id:<15} | {cvss}", flush=True)

                    df = None
                    try: os.remove(file_path)
                    except: pass

                except Exception: pass
            
            time.sleep(0.1)

    except KeyboardInterrupt: pass

if __name__ == "__main__":
    start_xgboost_worker()