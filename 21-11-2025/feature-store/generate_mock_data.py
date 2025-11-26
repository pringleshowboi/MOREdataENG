import pandas as pd
from datetime import datetime, timedelta
import os
import random

# --- Configuration ---
# Set the correct path relative to where you run this script (feature-store directory)
DATA_DIR = os.path.join("feature_repo", "data")
PARQUET_PATH = os.path.join(DATA_DIR, "user_features.parquet")

# --- 1. Create Data Directory ---
os.makedirs(DATA_DIR, exist_ok=True)
print(f"Ensured directory exists: {DATA_DIR}")

# --- 2. Generate Mock Data ---
start_time = datetime.now() - timedelta(days=30)
user_ids = list(range(100, 110)) # 10 distinct users

data = []
for user_id in user_ids:
    for i in range(5): # 5 events per user over time
        # Ensure the mock data has all columns defined in user_features.py
        data.append({
            "user_id": user_id,
            "event_timestamp": start_time + timedelta(hours=i * 5, minutes=random.randint(0, 59)),
            "total_logins": random.randint(10, 500),
            "average_session_duration": round(random.uniform(30.0, 3600.0), 2),
            "last_active_date": (datetime.now() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d"),
            "total_revenue": round(random.uniform(100.0, 50000.0), 2),
            "average_order_value": round(random.uniform(20.0, 500.0), 2),
            "lifetime_value": round(random.uniform(500.0, 150000.0), 2),
        })

df = pd.DataFrame(data)

# --- 3. Save to Parquet ---
df.to_parquet(PARQUET_PATH, index=False)
print(f"Successfully generated mock data and saved to: {PARQUET_PATH}")

# Optional: Print schema to verify column names and types match expectations
print("\nGenerated DataFrame Schema:")
print(df.dtypes)