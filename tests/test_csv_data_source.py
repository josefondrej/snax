from datetime import datetime

import pandas as pd
import pytest
from pandas._testing import assert_frame_equal

from snax.csv_data_source import CsvDataSource
from snax.entity import Entity
from snax.example_feature_repos.sports_feature_repo.nhl_games import data_path as original_nhl_data_path
from snax.example_feature_repos.users_with_nas_feature_repo.users_with_nas import \
    data_path as original_users_with_na_data_path
from snax.feature import Feature
from snax.utils import copy_to_temp
from snax.value_type import Int, String


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


def test_select_all_columns(users_with_nas_data_source):
    data = users_with_nas_data_source.select()
    assert isinstance(data, pd.DataFrame)


def test_insert_validates_argument(users_with_nas_data_source):
    with pytest.raises(ValueError) as exception_info:
        users_with_nas_data_source.insert(['id'], ['first_name'], pd.DataFrame(), if_exists='foobar')


def test_insert_full_new_column(users_with_nas_data_source):
    first_names_uppercase = [
        'CIRILLO', 'CODI', 'MARION', 'GERMAINE', None, 'MELLONEY', 'MADEL', None, 'ROCKWELL', 'TRUMAINE']

    users_with_nas_data_source.insert(
        key=[Entity('user', join_keys=['id'])],
        columns=[Feature('first_name_uppercase', String)],
        data=pd.DataFrame({
            'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'first_name_uppercase': first_names_uppercase
        })
    )
    data = users_with_nas_data_source.select(['first_name_uppercase'])
    assert set(first_names_uppercase) == set(list(data['first_name_uppercase']))


def test_insert_partial_new_column(users_with_nas_data_source):
    first_names_uppercase = ['MARION', None, 'TRUMAINE']

    users_with_nas_data_source.insert(
        key=[Entity('user', join_keys=['id'])],
        columns=[Feature('first_name_uppercase', String)],
        data=pd.DataFrame({
            'id': [3, 5, 10],
            'first_name_uppercase': first_names_uppercase
        })
    )
    data = users_with_nas_data_source.select(['first_name_uppercase'])
    assert (~data['first_name_uppercase'].isna()).sum() == len(first_names_uppercase)
    assert set(first_names_uppercase) == set(list(data['first_name_uppercase']))


def test_insert_full_new_row(users_with_nas_data_source):
    new_data = pd.DataFrame({
        'id': [0, 11],
        'first_name': ['CIRILLO', 'CODI'],
        'last_name': ['SEABORN', 'PANDAS'],
        'gender': ['Male', 'Female'],
        'timestamp': ['2022-08-17T10:41:25', datetime(2022, 8, 16, 15, 45, 31)],
        'age': [None, 56],
        'is_subscribed': [False, True],
        'children': [2, None],
    })
    users_with_nas_data_source.insert(
        key=[Entity('user', join_keys=['id'])],
        columns=[Feature('first_name', dtype=String),
                 'last_name', 'gender', 'timestamp', 'age', 'is_subscribed', 'children'],
        data=new_data
    )
    data = users_with_nas_data_source.select()
    retrieved_new_data = data[data['id'].isin([0, 11])]
    assert_frame_equal(new_data, retrieved_new_data)


def test_insert_partial_new_row(users_with_nas_data_source):
    data = pd.DataFrame({
        'id': [0, 11],
        'gender': ['Male', 'Female'],
    })
    users_with_nas_data_source.insert(
        key=[Entity('user', join_keys=['id'])],
        columns=['gender'],
        data=data,
        if_exists='error'
    )
    retrieved_data = users_with_nas_data_source.select([Entity('user', join_keys=['id']), Feature('gender', String)])
    retrieved_new_data = retrieved_data[retrieved_data['id'].isin([0, 11])]
    assert_frame_equal(data, retrieved_new_data[['id', 'gender']])


def test_insert_existing_data_replace(users_with_nas_data_source):
    data = pd.DataFrame({
        'id': [4, 5, 6],
        'first_name': [None, 'Smith', 'Smith']
    })
    users_with_nas_data_source.insert(
        key=['id'],
        columns=['first_name'],
        data=data,
        if_exists='replace'
    )
    retrieved_data = users_with_nas_data_source.select(['id', 'first_name'])
    retrieved_data.sort_values(by='id', inplace=True)
    updated_retrieved_data = retrieved_data[retrieved_data['id'].isin([4, 5, 6])]
    assert_frame_equal(data, updated_retrieved_data)


def test_insert_existing_data_error(users_with_nas_data_source):
    with pytest.raises(ValueError) as exception_info:
        data = pd.DataFrame({'id': [4], 'first_name': [None]})
        users_with_nas_data_source.insert(key=['id'], columns=['first_name'], data=data, if_exists='error')


def test_insert_existing_data_ignore(users_with_nas_data_source):
    data = pd.DataFrame({'id': [4], 'first_name': [None]})
    users_with_nas_data_source.insert(key=['id'], columns=['first_name'], data=data, if_exists='error')
    retrieved_data = users_with_nas_data_source.select(['id', 'first_name'])

    updated_retrieved_data = retrieved_data[retrieved_data['id'] == 4]
    assert str(updated_retrieved_data['first_name'][0]) == 'Germaine'
