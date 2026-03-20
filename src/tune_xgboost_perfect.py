import pandas as pd
import xgboost as xgb
from sklearn.model_selection import RandomizedSearchCV, StratifiedKFold
from sklearn.metrics import accuracy_score
import os
import time

def tune_perfect_xgboost():
    print(">>> [INIT] Initializing Advanced Hyperparameter Optimization Engine...", flush=True)

    # 1. Load Data
    data_path = "../data/hpc_dataset_flattened.csv"
    if not os.path.exists(data_path):
        print("CRITICAL ERROR: Data not found.")
        return

    print(">>> [LOAD] Loading Dataset...", flush=True)
    df = pd.read_csv(data_path)
    
    # Drop non-numeric columns (Standard cleanup)
    if 'hash' in df.columns: df = df.drop(columns=['hash'])
    
    X = df.drop(columns=['label'])
    y = df['label']

    # 2. Define the "Search Space" (The Grid)
    # This covers every important knob to turn in XGBoost.
    param_grid = {
        'n_estimators': [500, 1000],          # Number of trees
        'learning_rate': [0.01, 0.05, 0.1],   # Step size (smaller = more precise)
        'max_depth': [6, 10, 15],             # Tree depth (deeper = more complex)
        'min_child_weight': [1, 3, 5],        # Minimum samples in a leaf (controls overfitting)
        'gamma': [0, 0.1, 0.3],               # Regularization (penalty for complexity)
        'subsample': [0.7, 0.8, 0.9],         # Fraction of data used per tree
        'colsample_bytree': [0.7, 0.8, 0.9]   # Fraction of features used per tree
    }

    # 3. Configure the Optimizer
    # RandomizedSearchCV is smarter than GridSearch. It randomly samples 50 best combinations.
    # StratifiedKFold ensures each test batch has the same ratio of Malware/Benign as the real world.
    xgb_model = xgb.XGBClassifier(
        objective='binary:logistic',
        eval_metric='error',
        use_label_encoder=False,
        n_jobs=-1,
        random_state=42
    )

    random_search = RandomizedSearchCV(
        estimator=xgb_model,
        param_distributions=param_grid,
        n_iter=20,              # Test 20 distinct model architectures
        scoring='accuracy',     # Optimize for pure Accuracy
        n_jobs=-1,              # Use all CPU cores
        cv=StratifiedKFold(n_splits=3, shuffle=True, random_state=42), # 3-Fold Cross-Validation
        verbose=1,
        random_state=42
    )

    print(f">>> [TUNING] Testing 20 architectural variations (3-Fold CV)... This may take 2-5 minutes.", flush=True)
    start_time = time.time()
    
    # 4. RUN THE SEARCH
    random_search.fit(X, y)
    
    end_time = time.time()
    print(f">>> [DONE] Optimization finished in {end_time - start_time:.2f} seconds.")

    # 5. REPORT THE WINNER
    best_model = random_search.best_estimator_
    best_params = random_search.best_params_
    best_score = random_search.best_score_

    print("\n" + "="*80)
    print("🏆 MATHEMATICALLY PERFECTED MODEL CONFIGURATION")
    print("="*80)
    print(f"   FINAL ACCURACY:      {best_score*100:.2f}% (Cross-Validated)")
    print("-" * 80)
    print(f"   Best Learning Rate:  {best_params['learning_rate']}")
    print(f"   Best Max Depth:      {best_params['max_depth']}")
    print(f"   Best Trees:          {best_params['n_estimators']}")
    print(f"   Best Subsample:      {best_params['subsample']}")
    print(f"   Best Gamma:          {best_params['gamma']} (Regularization)")
    print("="*80)

    # 6. SAVE THE CHAMPION
    model_dir = "../models/xgboost_perfected"
    if not os.path.exists(model_dir):
        os.makedirs(model_dir)
        
    model_path = os.path.join(model_dir, "model.json")
    best_model.save_model(model_path)
    print(f">>> [SAVE] Champion Model saved to: {model_path}")

if __name__ == "__main__":
    tune_perfect_xgboost()