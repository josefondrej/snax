from tempfile import NamedTemporaryFile

from snax.data_sources.csv_data_source import CsvDataSource
from snax.example_feature_repos.sports_feature_repo.nhl_games import data_path as original_nhl_data_path
from snax.example_feature_repos.users_with_nas_feature_repo.users_with_nas import \
    data_path as original_users_with_na_data_path
from snax.utils import copy_to_temp


def create_nhl_games() -> CsvDataSource:
    return CsvDataSource(
        name='nhl_games_csv',
        csv_file_path=copy_to_temp(original_nhl_data_path)
    )


def create_users_with_nas() -> CsvDataSource:
    return CsvDataSource(
        name='users_with_nas_csv',
        csv_file_path=copy_to_temp(original_users_with_na_data_path)
    )


def create_users_with_nas_field_mapping() -> CsvDataSource:
    return CsvDataSource(
        name='users_with_nas_csv',
        csv_file_path=copy_to_temp(original_users_with_na_data_path),
        field_mapping={
            'is_subscribed': 'issubscribed',
            'timestamp': 'time_stamp',
        }
    )


def create_empty_data_source() -> CsvDataSource:
    tmp_file = NamedTemporaryFile(suffix='.csv')
    return CsvDataSource(
        name='empty_csv',
        csv_file_path=tmp_file.name
    )
