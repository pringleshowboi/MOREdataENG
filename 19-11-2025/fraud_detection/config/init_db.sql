-- Drop the table if it already exists to allow for clean restarts during development
DROP TABLE IF EXISTS historical_transactions;

-- Create the table for the batch fraud detection dataset (creditcard.csv)
CREATE TABLE historical_transactions (
    -- Primary key for reliable row identification
    id SERIAL PRIMARY KEY,
    
    -- Features from the dataset
    "Time" FLOAT NOT NULL,
    "V1" FLOAT NOT NULL,
    "V2" FLOAT NOT NULL,
    "V3" FLOAT NOT NULL,
    "V4" FLOAT NOT NULL,
    "V5" FLOAT NOT NULL,
    "V6" FLOAT NOT NULL,
    "V7" FLOAT NOT NULL,
    "V8" FLOAT NOT NULL,
    "V9" FLOAT NOT NULL,
    "V10" FLOAT NOT NULL,
    "V11" FLOAT NOT NULL,
    "V12" FLOAT NOT NULL,
    "V13" FLOAT NOT NULL,
    "V14" FLOAT NOT NULL,
    "V15" FLOAT NOT NULL,
    "V16" FLOAT NOT NULL,
    "V17" FLOAT NOT NULL,
    "V18" FLOAT NOT NULL,
    "V19" FLOAT NOT NULL,
    "V20" FLOAT NOT NULL,
    "V21" FLOAT NOT NULL,
    "V22" FLOAT NOT NULL,
    "V23" FLOAT NOT NULL,
    "V24" FLOAT NOT NULL,
    "V25" FLOAT NOT NULL,
    "V26" FLOAT NOT NULL,
    "V27" FLOAT NOT NULL,
    "V28" FLOAT NOT NULL,
    
    -- Transaction amount
    "Amount" FLOAT NOT NULL,
    
    -- Target variable (0: Non-fraud, 1: Fraud)
    "Class" INTEGER NOT NULL
);

-- Optional: Create an index on the Class column for faster querying during model training
CREATE INDEX idx_class ON historical_transactions ("Class");

-- Table to store model predictions and results (we'll use this later)
DROP TABLE IF EXISTS model_predictions;

CREATE TABLE model_predictions (
    prediction_id SERIAL PRIMARY KEY,
    transaction_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    model_name VARCHAR(100) NOT NULL,
    is_fraud_predicted BOOLEAN NOT NULL,
    prediction_score FLOAT,
    data_source VARCHAR(50) NOT NULL -- 'batch' or 'stream'
);