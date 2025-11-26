# 01_training_retrieval.py: Demonstrates how to fetch historical feature data for model training
from datetime import datetime, timedelta
import pandas as pd
from feast import FeatureStore

# 1. Initialize the Feature Store
# This looks for feature_store.yaml in the current directory, which is feature_repo/
store = FeatureStore(repo_path="feature_repo")

print("1. Initializing Feast FeatureStore...")

# 2. Define the training dataset (Entity Data)
# This DataFrame contains the entities (user_id) and the timestamps 
# (event_timestamp) for which we want features.
end_time = datetime.now()
entity_df = pd.DataFrame.from_dict({
    "user_id": [1, 2, 3, 4, 5],
    "event_timestamp": [
        end_time - timedelta(days=2),
        end_time - timedelta(days=3),
        end_time - timedelta(days=4),
        end_time - timedelta(days=5),
        end_time - timedelta(days=6),
    ],
    # Add a dummy label column for training simulation
    "label_is_churn": [0, 1, 0, 1, 0],
})

print(f"2. Entity DataFrame (5 observations):\n{entity_df.head()}")

# 3. Define the features we want to retrieve
# We use the Feature Service defined in user_features.py
feature_service = store.get_feature_service("user_feature_service")

# 4. Retrieve historical features from the Offline Store (Postgres)
print("\n3. Retrieving historical features from Postgres (Offline Store)...")

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=feature_service,
).to_df()

# 5. Display the result
print("\n4. Retrieved Training DataFrame:")
print(training_df.head())

# The resulting DataFrame (training_df) is now ready to be used
# to train a model (e.g., training_df.to_csv("training_data.csv"))

print("\nSuccess! The retrieved data includes features joined with entity data and labels.")