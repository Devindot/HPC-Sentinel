import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import os

def train_perfect_xgboost():
    print(">>> [INIT] Initializing XGBoost 'Champion' Trainer...", flush=True)

    # 1. Load Data
    data_path = "../data/hpc_dataset_flattened.csv"
    if not os.path.exists(data_path):
        print("CRITICAL ERROR: Data not found.")
        return

    print(">>> [LOAD] Loading Dataset into Memory...", flush=True)
    df = pd.read_csv(data_path)
    
    # CRITICAL FIX: Drop the 'hash' string column if it exists
    if 'hash' in df.columns:
        print(">>> [CLEAN] Dropping non-numeric 'hash' identifier...", flush=True)
        df = df.drop(columns=['hash'])

    # 2. Prepare Features (X) and Target (y)
    # We assume 'label' is the target. All other columns are features.
    X = df.drop(columns=['label'])
    y = df['label']

    # 3. Split Data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # 4. Configure the "Perfect" Model
    # Optimized for Deep Pattern Recognition
    model = xgb.XGBClassifier(
        n_estimators=1000,       # Max trees (will stop early)
        learning_rate=0.05,      # Low learning rate for precision
        max_depth=12,            # Deep trees
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="error",
        early_stopping_rounds=50, # Stop if no improvement
        use_label_encoder=False,
        random_state=42,
        n_jobs=-1
    )

    print(">>> [TRAIN] Training with Early Stopping...", flush=True)
    
    # 5. Train
    eval_set = [(X_test, y_test)]
    model.fit(
        X_train, y_train,
        eval_set=eval_set,
        verbose=True
    )

    # 6. Evaluate
    print("\n>>> [EVAL] Calculating Final Accuracy...", flush=True)
    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    
    print("\n" + "="*60)
    print(f"🏆 XGBOOST FINAL ACCURACY: {acc*100:.2f}%")
    print("="*60)
    
    # 7. Save
    model_dir = "../models/xgboost_champion"
    if not os.path.exists(model_dir):
        os.makedirs(model_dir)
        
    model_path = os.path.join(model_dir, "model.json")
    model.save_model(model_path)
    print(f">>> [SAVE] Model saved to: {model_path}")

if __name__ == "__main__":
    train_perfect_xgboost()