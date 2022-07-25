from pathlib import Path

from snax.data_sources.csv_data_source import CsvDataSource
from snax.entity import Entity
from snax.feature import Feature
from snax.feature_view import FeatureView
from snax.value_type import Int, String, Timestamp, Float, Bool

data_path = Path(__file__).parent.parent.parent / 'data' / 'users_with_nas.csv'

# Define DataSource Objects --------------------------------------------------------------------------------------------
users_with_nas_csv_data_source = CsvDataSource(
    name='users_with_nas_csv',
    csv_file_path=data_path
)

# Define Entity Objects ------------------------------------------------------------------------------------------------
user = Entity(
    name='user',
    join_keys=['id']
)

# Define FeatureView Objects -------------------------------------------------------------------------------------------
users_with_nas_csv_feature_view = FeatureView(
    name='users_with_nas_csv',
    entities=[user],
    features=[
        Feature('id', Int),
        Feature('first_name', String),
        Feature('last_name', String),
        Feature('gender', String),
        Feature('timestamp', Timestamp),
        Feature('age', Float),
        Feature('is_subscribed', Bool),
        Feature('children', Int),
    ],
    source='users_with_nas_csv_data_source'
)
