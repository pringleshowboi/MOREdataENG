#!/bin/bash
# Save as: airflow/entrypoint.sh

set -e

echo "🔧 Installing ML packages..."

# Install required packages
pip install --no-cache-dir \
    pandas>=2.0.0 \
    numpy>=1.24.0 \
    scikit-learn>=1.3.0 \
    xgboost>=2.0.0 \
    imbalanced-learn>=0.11.0 \
    mlflow>=2.10.0 \
    confluent-kafka>=2.3.0 \
    azure-storage-blob>=12.19.0 \
    joblib>=1.3.0

echo "✅ ML packages installed"

# Verify critical packages
python -c "import imblearn; print(f'✅ imbalanced-learn {imblearn.__version__}')" || exit 1
python -c "import mlflow; print(f'✅ mlflow {mlflow.__version__}')" || exit 1

echo "🚀 Starting Airflow..."

# Execute the original entrypoint
exec /entrypoint "$@"