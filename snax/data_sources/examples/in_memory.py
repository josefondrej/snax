import pandas as pd

from snax.data_sources.csv_data_source import CsvDataSource
from snax.data_sources.in_memory_data_source import InMemoryDataSource
from snax.example_feature_repos.sports_feature_repo.nhl_games import data_path as original_nhl_data_path
from snax.example_feature_repos.users_with_nas_feature_repo.users_with_nas import \
    data_path as original_users_with_na_data_path


def create_nhl_games() -> CsvDataSource:
    return InMemoryDataSource(
        name='nhl_games_in_memory',
        data=pd.read_csv(original_nhl_data_path)
    )


def create_users_with_nas() -> CsvDataSource:
    return InMemoryDataSource(
        name='users_with_nas_in_memory',
        data=pd.read_csv(original_users_with_na_data_path)
    )


def create_users_with_nas_field_mapping() -> CsvDataSource:
    return InMemoryDataSource(
        name='users_with_nas_in_memory',
        data=pd.read_csv(original_users_with_na_data_path),
        field_mapping={
            'is_subscribed': 'issubscribed',
            'timestamp': 'time_stamp',
        }
    )
