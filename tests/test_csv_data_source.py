import pandas as pd
import pytest

from snax.csv_data_source import CsvDataSource
from snax.entity import Entity
from snax.example_feature_repos.sports_feature_repo.nhl_games import data_path as original_nhl_data_path
from snax.example_feature_repos.users_with_nas_feature_repo.users_with_nas import \
    data_path as original_users_with_na_data_path
from snax.feature import Feature
from snax.utils import copy_to_temp
from snax.value_type import Int


@pytest.fixture
def nhl_data_source():
    data_path = copy_to_temp(original_nhl_data_path)

    data_source = CsvDataSource(
        name='nhl_games_csv',
        csv_file_path=data_path
    )

    return data_source


@pytest.fixture
def users_with_nas_data_source():
    data_path = copy_to_temp(original_users_with_na_data_path)

    data_source = CsvDataSource(
        name='users_with_nas_csv',
        csv_file_path=data_path
    )

    return data_source


def test_select_single_column_by_string(nhl_data_source):
    data = nhl_data_source.select(['game_id'])
    first_few_sorted_game_ids = sorted(list(data['game_id']))[:5]
    expected_first_few_sorted_game_ids = [2015020007, 2015020061, 2015020079, 2015020086, 2015020165]
    assert first_few_sorted_game_ids == expected_first_few_sorted_game_ids


def test_select_single_column_by_feature(nhl_data_source):
    data = nhl_data_source.select([Feature('game_id', Int)])
    first_few_sorted_game_ids = sorted(list(data['game_id']))[:5]
    expected_first_few_sorted_game_ids = [2015020007, 2015020061, 2015020079, 2015020086, 2015020165]
    assert first_few_sorted_game_ids == expected_first_few_sorted_game_ids


def test_select_single_column_by_entity(nhl_data_source):
    data = nhl_data_source.select([Entity('game', join_keys=['game_id'])])
    first_few_sorted_game_ids = sorted(list(data['game_id']))[:5]
    expected_first_few_sorted_game_ids = [2015020007, 2015020061, 2015020079, 2015020086, 2015020165]
    assert first_few_sorted_game_ids == expected_first_few_sorted_game_ids


def test_select_multiple_columns(nhl_data_source):
    data = nhl_data_source.select(
        columns=[
            Entity('game', join_keys=['game_id']),
            Feature('game_id', Int),
            'game_id',
            Feature('away_team_id', Int)
        ]
    )
    expected_data_columns = ['game_id', 'game_id', 'game_id', 'away_team_id']
    assert list(data.columns) == expected_data_columns


def test_select_string_column_with_missings(users_with_nas_data_source):
    data = users_with_nas_data_source.select(['first_name'])
    assert isinstance(data, pd.DataFrame)


def test_select_int_column_with_missings(users_with_nas_data_source):
    data = users_with_nas_data_source.select(['children'])
    assert isinstance(data, pd.DataFrame)


def test_select_float_column_with_missings(users_with_nas_data_source):
    data = users_with_nas_data_source.select(['age'])
    assert isinstance(data, pd.DataFrame)


def test_select_timestamp_column_with_missings(users_with_nas_data_source):
    data = users_with_nas_data_source.select(['timestamp'])
    assert isinstance(data, pd.DataFrame)
