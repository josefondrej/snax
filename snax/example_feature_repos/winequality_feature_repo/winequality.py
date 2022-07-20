from pathlib import Path

from snax.csv_data_source import CsvDataSource
from snax.feature import Feature
from snax.feature_view import FeatureView
from snax.value_type import Float

data_path = Path(__file__).parent.parent / 'data' / 'winequality.csv'

# Define DataSource Objects --------------------------------------------------------------------------------------------
data_source = CsvDataSource(
    name='winequality_csv',
    csv_file_path=data_path,
    separator=',',
    field_mapping={
        'citric_acid': 'citric_acidity',
        'pH': 'ph'
    },
    tags={
        'owner': 'john.doe@gmail.com'
    }
)

# Define FeatureView Objects -------------------------------------------------------------------------------------------
winequality_acidity_feature_view = FeatureView(
    name='winequality_acidity_csv',
    entities=None,
    features=[
        Feature('volatile_acidity', Float),
        Feature('fixed_acidity', Float),
        Feature('citric_acidity', Float),
        Feature('ph', Float)
    ],
    source=data_source
)
