from pathlib import Path

import snax.example_feature_repos.winequality_feature_repo as winequality_feature_repo
from snax.data_sources.csv_data_source import CsvDataSource
from snax.feature import Feature
from snax.feature_view import FeatureView
from snax.repo_contents import parse_repo, DUMMY_ENTITY
from snax.utils import copy_to_temp
from snax.value_type import Float


def test_parse_repo():
    # Copy to arbitrary location to simulate real-world use-case
    original_wine_quality_repo_path = Path(winequality_feature_repo.__file__).parent
    wine_quality_repo_path = copy_to_temp(original_wine_quality_repo_path)

    repo_contents = parse_repo(repo_path=wine_quality_repo_path)

    assert len(repo_contents.entities) == 1
    assert len(repo_contents.data_sources) == 1
    assert len(repo_contents.feature_views) == 1

    expected_entity = DUMMY_ENTITY
    expected_data_source = CsvDataSource(
        name='winequality_csv',
        csv_file_path=None
    )
    expected_feature_view = FeatureView(
        name='winequality_acidity_csv',
        entities=None,
        features=[
            Feature('volatile_acidity', Float),
            Feature('fixed_acidity', Float),
            Feature('citric_acidity', Float),
            Feature('ph', Float)
        ],
        source=expected_data_source
    )

    assert repo_contents.entities[0] == expected_entity
    assert repo_contents.data_sources[0] == expected_data_source
    assert repo_contents.feature_views[0] == expected_feature_view
