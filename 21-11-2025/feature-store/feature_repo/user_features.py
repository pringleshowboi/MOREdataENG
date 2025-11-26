from datetime import timedelta

# Import the necessary components
from feast import Entity, FeatureService, FeatureView, Field, ValueType
from feast.types import Int64, Float64, String
# Import the FileSource object, compatible with LocalOfflineStore
from feast.infra.offline_stores.file_source import FileSource 

# Entity definition
user_entity = Entity(
    name="user_id",
    description="User ID",
    value_type=ValueType.INT64,
)

# Data source definition: Changed to FileSource to match LocalOfflineStore
# This assumes your mock data file is at 'data/user_features.parquet'
user_data_source = FileSource(
    path="data/user_features.parquet",
    # This column is required for point-in-time joins
    event_timestamp_column="event_timestamp",
)

# Feature view definition (user_activity_feature)
user_activity_feature = FeatureView(
    name="user_activity",
    entities=[user_entity],
    ttl=timedelta(weeks=51),
    source=user_data_source, 
    tags={},
    schema=[
        Field(name="total_logins", dtype=Int64),
        Field(name="average_session_duration", dtype=Float64),
        Field(name="last_active_date", dtype=String),
    ],
)

# Sales-related feature view (user_sales_feature)
user_sales_feature = FeatureView(
    name="user_profitability",
    entities=[user_entity],
    ttl=timedelta(weeks=52),
    source=user_data_source,
    tags={},
    schema=[
        Field(name="total_revenue", dtype=Float64), 
        Field(name="average_order_value", dtype=Float64),
        Field(name="lifetime_value", dtype=Float64),
    ],
)

# Feature service
user_service = FeatureService(
    name="user_service",
    features=[user_activity_feature, user_sales_feature],
)