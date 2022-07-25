from pathlib import Path

from snax.data_sources.csv_data_source import CsvDataSource
from snax.feature import Feature
from snax.feature_view import FeatureView
from snax.value_type import Float

data_path = Path(__file__).parent.parent.parent / 'data' / 'winequality.csv'

# Define DataSource Objects --------------------------------------------------------------------------------------------
winequality_csv_data_source = CsvDataSource(
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
winequality_csv_acidity_feature_view = FeatureView(
    name='winequality_acidity_csv',
    entities=None,
    features=[
        Feature('volatile_acidity', Float),
        Feature('fixed_acidity', Float),
        Feature('citric_acidity', Float),
        Feature('ph', Float)
    ],
    source=winequality_csv_data_source
)
