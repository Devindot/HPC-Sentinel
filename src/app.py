import streamlit as st
import pandas as pd
import subprocess
import time
import os
import sys
import sqlite3
import shutil

# --- CONFIGURATION ---
st.set_page_config(
    page_title="HPC-Sentinel",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- PATHS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "dashboard.db")
SAFE_ROOT = os.path.join(os.environ.get("TEMP", "C:\\Temp"), "HPC_Sentinel_Live")

# --- PROFESSIONAL UI STYLING ---
st.markdown("""
    <style>
    /* 1. IMPORT POPPINS FONT */
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600;700&display=swap');

    /* 2. GLOBAL RESET & FONT */
    html, body, [class*="css"] {
        font-family: 'Poppins', sans-serif;
        color: #ffffff;
    }
    
    /* 3. BACKGROUND */
    .stApp {
        background: radial-gradient(circle at center, #1b2735 0%, #090a0f 100%);
    }

    /* 4. GLASS CARD - CENTERED TEXT */
    .glass-card {
        background: rgba(255, 255, 255, 0.03);
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        border-radius: 24px;
        border: 1px solid rgba(255, 255, 255, 0.08);
        padding: 40px 20px;
        margin-bottom: 25px;
        box-shadow: 0 4px 30px rgba(0, 0, 0, 0.1);
        text-align: center;
    }

    /* 5. TYPOGRAPHY HIERARCHY */
    .main-title {
        font-size: 3.5rem;
        font-weight: 700;
        background: -webkit-linear-gradient(left, #4ca1af, #c4e0e5);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 10px;
    }
    
    .sub-title {
        font-size: 1.2rem;
        font-weight: 300;
        color: #cfd8dc;
        margin-bottom: 0px;
    }

    .section-header {
        font-size: 2rem;
        font-weight: 600;
        margin-top: 30px;
        margin-bottom: 20px;
        text-align: center;
        color: #ffffff;
        border-bottom: 2px solid rgba(255,255,255,0.1);
        padding-bottom: 10px;
        display: inline-block;
    }
    
    .status-text {
        font-size: 0.9rem;
        letter-spacing: 1px;
        font-weight: 600;
        color: #00e676;
    }

    /* 6. BUTTON STYLING (FIXED PADDING) */
    .stButton>button {
        background: rgba(255, 255, 255, 0.05);
        color: white;
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        padding: 15px 32px; /* INCREASED SIDE PADDING */
        font-weight: 500;
        font-family: 'Poppins', sans-serif;
        transition: all 0.3s ease;
        text-transform: uppercase;
        letter-spacing: 1.5px; /* More spacing between letters */
        min-width: 200px;      /* Ensure buttons are wide enough */
    }
    .stButton>button:hover {
        background: rgba(255, 255, 255, 0.15);
        border-color: #4ca1af;
        transform: translateY(-2px);
        box-shadow: 0 5px 15px rgba(0,0,0,0.3);
    }

    /* 7. TABLE CLEANUP */
    .dataframe {
        font-family: 'Poppins', sans-serif !important;
        font-size: 0.9rem !important;
    }
    
    /* Center the table container */
    [data-testid="stDataFrame"] {
        width: 100%;
        display: flex;
        justify-content: center;
    }
    </style>
""", unsafe_allow_html=True)

# --- SESSION STATE ---
if 'running' not in st.session_state: st.session_state['running'] = False
if 'procs' not in st.session_state: st.session_state['procs'] = []

# --- FUNCTIONS ---
def start_system():
    if st.session_state['running']: return
    
    # 1. Safe Cleanup
    if os.name == 'nt': 
        os.system("taskkill /f /im java.exe >nul 2>&1")

    # 2. Reset Folders
    if os.path.exists(SAFE_ROOT):
        try: shutil.rmtree(SAFE_ROOT)
        except: pass
    if not os.path.exists(SAFE_ROOT): os.makedirs(SAFE_ROOT)
    
    # 3. Reset DB
    if os.path.exists(DB_PATH):
        try: os.remove(DB_PATH)
        except: pass

    # 4. Launch Processes
    python_cmd = sys.executable 
    
    p1 = subprocess.Popen([python_cmd, os.path.join(BASE_DIR, "4a_spark_feeder.py")])
    time.sleep(6) 
    p2 = subprocess.Popen([python_cmd, os.path.join(BASE_DIR, "4b_xgboost_worker.py")])
    time.sleep(2)
    p3 = subprocess.Popen([python_cmd, os.path.join(BASE_DIR, "3_real_producer.py")])
    
    st.session_state['procs'] = [p1, p2, p3]
    st.session_state['running'] = True

def stop_system():
    if not st.session_state['running']: return
    for p in st.session_state['procs']:
        p.terminate()
        try: p.wait(timeout=1)
        except: p.kill()
    if os.name == 'nt': os.system("taskkill /f /im java.exe >nul 2>&1")
    st.session_state['procs'] = []
    st.session_state['running'] = False

# --- UI LAYOUT ---

# 1. Header Section
st.markdown("""
<div class="glass-card">
    <div class="main-title">🛡️ HPC-SENTINEL</div>
    <div class="sub-title">Real-Time High Performance Computing Telemetry Malware Detection</div>
    <br>
    <p style="font-size: 0.9rem; color: #aaa;"><b>Architecture:</b> Atomic Producer → Spark Structured Streaming → XGBoost Inference</p>
</div>
""", unsafe_allow_html=True)

# 2. Controls
c1, c2, c3 = st.columns([1, 2, 1]) 
with c1:
    if st.button("▶ START SYSTEM"):
        start_system()
        st.rerun()
with c3:
    if st.button("⏹ STOP SYSTEM"):
        stop_system()
        st.rerun()

# 3. Status Bar
st.markdown("<div style='text-align: center; margin-top: 10px;'>", unsafe_allow_html=True)
if st.session_state['running']:
    st.markdown('<p class="status-text">⚡ SYSTEM ONLINE: INGESTING LIVE TELEMETRY</p>', unsafe_allow_html=True)
else:
    st.markdown('<p class="status-text" style="color: #ff9800;">💤 SYSTEM OFFLINE: AWAITING COMMAND</p>', unsafe_allow_html=True)
st.markdown("</div>", unsafe_allow_html=True)

# 4. Live Feed Section
st.markdown("<div style='text-align: center;'><div class='section-header'>📡 Live Threat Feed</div></div>", unsafe_allow_html=True)

placeholder = st.empty()

# --- MAIN LOOP ---
if st.session_state['running']:
    while True:
        if os.path.exists(DB_PATH):
            try:
                conn = sqlite3.connect(DB_PATH)
                # UPDATED: Pulling the two new columns from the DB
                query = "SELECT timestamp, filename_hash, threat_score, status, cve_id, cvss_score FROM feed ORDER BY rowid DESC LIMIT 10"
                df = pd.read_sql_query(query, conn)
                conn.close()
                
                if not df.empty:
                    # UPDATED: Mapping the 6 headers
                    df.columns = ["TIME", "FILE / HASH", "AI SCORE", "STATUS", "CVE ID", "CVSS"]
                    
                    def color_status(val):
                        if "MALWARE" in val: return 'color: #ff4b4b; font-weight: bold;'
                        if "SUSPICIOUS" in val: return 'color: #ffa700; font-weight: bold;'
                        return 'color: #00e676; font-weight: bold;'
                        
                    # NEW: Color logic for CVSS severity
                    def color_cvss(val):
                        try:
                            score = float(val)
                            if score >= 9.0: return 'color: #ff4b4b; font-weight: bold;'
                            if score >= 7.0: return 'color: #ffa700; font-weight: bold;'
                            if score > 0.0:  return 'color: #ffd600;'
                            return 'color: #00e676;'
                        except: return ''
                    
                    with placeholder.container():
                        st.dataframe(
                            df.style.map(color_status, subset=['STATUS'])
                                    .map(color_cvss, subset=['CVSS']), 
                            use_container_width=True, 
                            hide_index=True
                        )
            except Exception as e:
                pass
        time.sleep(0.5)