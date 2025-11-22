🛡️ MLOps Fraud Detection Dashboard

Project Summary

This project establishes a robust, end-to-end dashboard designed for monitoring a machine learning-powered fraud detection system. It serves two distinct audiences:

Fraud Operations Analysts: Who require real-time alerts and performance indicators to manage daily operations.

Data Scientists/ML Engineers: Who need MLOps metrics to track model health, detect data drift, and monitor model performance over time.

Data Model Overview

The dashboard integrates three data sources to create a unified view, leveraging DAX measures for dynamic calculation and performance tracking.

Table

Purpose

Relationship

Fact_RealTimeScores

Stores every transaction and the model's fraud probability score (fraud_probability) and the outcome (is_fraud). (Fact Table)

Many-to-One (version)

Dim_Model

Stores model metadata, version history, and current deployment stage (e.g., 'Production', 'Staging') from an MLflow-like registry. (Dimension Table)

One-to-Many (version)

Fact_Metrics

Stores offline performance metrics (ROC-AUC, F1-Score, Precision) for every historical model version. (Fact Table)

Many-to-One (version)

creditcard_training

Provides a baseline dataset representing the model's original training data for drift detection.

No Active Relationship

Key DAX Measures

These calculations are the backbone of the dashboard's KPIs and logic:

Total_Transactions: Counts total volume for operational monitoring.

Live_Fraud_Count: Counts transactions where the model predicted fraud (is_fraud = 1).

Live_Fraud_Rate: Calculates the percentage of fraud alerts ([Live_Fraud_Count] / [Total_Transactions]).

Average_Confidence: Calculates the model's mean prediction certainty for a health check.

Current_Model_Version: Identifies the version of the model currently in production.

Dashboard Pages

1. 🟢 Real-Time Operational Monitor (Audience: Analysts)

This page focuses on immediate action and current performance.

Visual

Data Source / Measure

Function

KPI Cards

[Total_Transactions], [Live_Fraud_Count], [Live_Fraud_Rate], [Average_Confidence], [Current_Model_Version]

Provides a single-glance overview of volume and performance against operational targets.

Map Visual

location field (Size by [Total_Transactions])

Identifies the geographic origin of live transaction flow and fraud alerts.

Live Alerts Table

Transaction details (ID, Amount, Probability)

Crucially filtered to only show high-confidence fraud alerts (e.g., fraud_probability $\ge 0.85$) to focus analyst effort.

Volume Trend Chart

[Total_Transactions] vs. scored_at (time)

Confirms data pipeline health and transaction flow consistency.

2. 📈 Model Performance History (Audience: ML Engineers)

This page tracks the success and quality of models over the project lifecycle, using data from the Fact_Metrics table.

Visual

Data Source

Function

ROC-AUC Trend

Fact_Metrics[roc_auc] vs. Dim_Model[registration_date]

Monitors the model's core quality metric (ROC-AUC) across deployed versions, indicating long-term performance improvements or degradation.

F1-Score Comparison

Fact_Metrics[f1_score] vs. Dim_Model[version]

Compares specific versions side-by-side using a key business-aligned metric (F1-Score).

Production Version Card

[Current_Model_Version] (Filtered by current_stage = 'Production')

Provides the crucial link between the historic performance data and the version currently handling live traffic.

3. 📉 Data Drift and Verification (Audience: Data Scientists)

This page monitors the relationship between the incoming live data and the original training data to detect feature drift.

Visual

Data Source

Function

Amount Distribution Comparison

Dual-axis chart comparing Count of Live Data vs. Count of Training Data grouped by amount (bins).

The primary visual for Data Drift detection. A significant divergence indicates the need for model retraining.

Confidence Trend (KPI)

[Average_Confidence] vs. scored_at (time)

Detects Concept Drift. A sudden, sustained drop in confidence implies the model is seeing brand-new patterns or features.

Location Distribution

Two side-by-side pie charts: one for creditcard_training location counts, and one for Fact_RealTimeScores location counts.

Compares the geographic mix of the current data against the baseline to catch emerging regional patterns.