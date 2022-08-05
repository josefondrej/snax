from typing import Union
from unittest import skip

import pandas as pd
import pytest
from _pytest.outcomes import Skipped
from pandas.testing import assert_frame_equal

from snax.data_sources.data_source_base import DataSourceBase
from snax.data_sources.examples import csv, in_memory, oracle
from snax.data_sources.oracle_data_source import OracleDataSource
from snax.entity import Entity
from snax.feature import Feature
from snax.utils import frames_equal_up_to_row_ordering
from snax.value_type import Int, String, Bool, Timestamp

_data_source_backend_to_examples_module = {
    'csv': csv,
    'in-memory': in_memory,
    'oracle': oracle
}

_data_source_backends = ['csv', 'in-memory', 'oracle']


def _handle_unavailable_datasource(data_source_backend: str, data_source: DataSourceBase) -> Union[
    Skipped, DataSourceBase]:
    if data_source_backend == 'oracle' and data_source is None:
        return pytest.skip('Oracle backend is not available')
    else:
        return data_source


@pytest.fixture(params=_data_source_backends)
def nhl_data_source(request):
    module = _data_source_backend_to_examples_module[request.param]
    data_source = module.create_nhl_games()
    return _handle_unavailable_datasource(request.param, data_source)


@pytest.fixture(params=_data_source_backends)
def users_with_nas_data_source(request):
    module = _data_source_backend_to_examples_module[request.param]
    data_source = module.create_users_with_nas()
    return _handle_unavailable_datasource(request.param, data_source)


@pytest.fixture(params=_data_source_backends)
def users_with_nas_field_mapping_data_source(request):
    module = _data_source_backend_to_examples_module[request.param]
    data_source = module.create_users_with_nas_field_mapping()
    return _handle_unavailable_datasource(request.param, data_source)


@pytest.fixture(params=_data_source_backends)
def empty_data_source(request):
    module = _data_source_backend_to_examples_module[request.param]
    data_source = module.create_empty_data_source()
    return _handle_unavailable_datasource(request.param, data_source)


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


def test_select_by_key_values_single_int_index(nhl_data_source):
    key_values = pd.DataFrame({'game_id': [2016020045, 2017020812, 2015020314]})
    selected_data = nhl_data_source.select(
        columns=['game_id', 'season', 'type'], key=['game_id'], key_values=key_values)
    expected_selected_data = pd.DataFrame({
        'game_id': [2016020045, 2017020812, 2015020314],
        'season': [20162017, 20172018, 20152016],
        'type': ['R', 'R', 'R']
    })
    assert frames_equal_up_to_row_ordering(selected_data, expected_selected_data)


def test_select_by_key_values_single_int_index(nhl_data_source):
    if isinstance(nhl_data_source, OracleDataSource):
        # TODO: Implement corresponding behavior for Oracle
        return skip('Handling case-sensitive column names not yet implemented for Oracle data source')

    key_values = pd.DataFrame({'game_id': [2016020045, 2017020812, 2015020314]})
    selected_data = nhl_data_source.select(
        columns=['game_id', 'date_time_GMT'], key=['game_id'], key_values=key_values)
    expected_selected_data = pd.DataFrame({
        'game_id': [2016020045, 2017020812, 2015020314],
        'date_time_GMT': ['2016-10-19T00:30:00Z', '2018-02-07T00:00:00Z', '2015-11-24T01:00:00Z']
    })
    assert frames_equal_up_to_row_ordering(selected_data, expected_selected_data)


def test_select_by_key_values_single_string_index(users_with_nas_data_source):
    key_values = pd.DataFrame({'string_id': ['a', 'b', 'c']})
    selected_data = users_with_nas_data_source.select(
        columns=['string_id', 'id', 'first_name'], key=['string_id'], key_values=key_values)
    expected_selected_data = pd.DataFrame({
        'string_id': ['a', 'b', 'c'],
        'id': [1, 2, 3],
        'first_name': ['Cirillo', 'Codi', 'Marion']
    })
    assert frames_equal_up_to_row_ordering(selected_data, expected_selected_data)


def test_select_by_key_values_string_int_index(users_with_nas_data_source):
    key_values = pd.DataFrame({'string_id': ['a', 'b', 'c'], 'id': [1, 2, 3]})
    selected_data = users_with_nas_data_source.select(
        columns=['string_id', 'id', 'first_name'], key=['string_id', 'id'], key_values=key_values)
    expected_selected_data = pd.DataFrame({
        'string_id': ['a', 'b', 'c'],
        'id': [1, 2, 3],
        'first_name': ['Cirillo', 'Codi', 'Marion']
    })
    assert frames_equal_up_to_row_ordering(selected_data, expected_selected_data)


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
    data = users_with_nas_data_source.select(['id', 'first_name_uppercase'])
    assert (~data['first_name_uppercase'].isna()).sum() == 2
    data_updated = data[data['id'].isin([3, 5, 10])]
    data_updated.sort_values(by='id', inplace=True)
    assert first_names_uppercase == list(data_updated['first_name_uppercase'])


def test_insert_full_new_row(users_with_nas_data_source):
    new_data = pd.DataFrame({
        'id': [0, 11],
        'first_name': ['CIRILLO', 'CODI'],
        'last_name': ['SEABORN', 'PANDAS'],
        'gender': ['Male', 'Female'],
        'timestamp': ['2022-08-17T10:41:25', '2022-08-16T15:45:31'],
        'age': [None, 56],
        'is_subscribed': [False, True],
        'children': [2, None],
        'string_id': ['x', 'y']
    })
    users_with_nas_data_source.insert(
        key=[Entity('user', join_keys=['id'])],
        columns=[Feature('first_name', dtype=String),
                 'last_name', 'gender', 'timestamp', 'age', 'is_subscribed', 'children', 'string_id'],
        data=new_data
    )
    retrieved_data = users_with_nas_data_source.select()
    filtered_retrieved_data = retrieved_data[retrieved_data['id'].isin([0, 11])]
    assert_frame_equal(new_data.reset_index(drop=True), filtered_retrieved_data.reset_index(drop=True),
                       check_dtype=False)


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
    filtered_retrieved_data = retrieved_data[retrieved_data['id'].isin([0, 11])]
    assert_frame_equal(data.reset_index(drop=True), filtered_retrieved_data[['id', 'gender']].reset_index(drop=True))


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
    filtered_retrieved_data = retrieved_data[retrieved_data['id'].isin([4, 5, 6])]
    assert_frame_equal(data.reset_index(drop=True), filtered_retrieved_data.reset_index(drop=True))


def test_insert_multikey_int_int(nhl_data_source):
    data = pd.DataFrame({
        'game_id': [2016020045, 2017020812, 2015020314],
        'season': [20162017, 20172018, 20152016],
        'home_goals': [3, 2, 1],
    })
    nhl_data_source.insert(
        key=['game_id', 'season'],
        columns=['home_goals'],
        data=data,
        if_exists='replace'
    )
    retrieved_data = nhl_data_source.select(['game_id', 'season', 'home_goals'])
    filtered_retrieved_data = retrieved_data[retrieved_data['game_id'].isin([2016020045, 2017020812, 2015020314])]
    assert frames_equal_up_to_row_ordering(data, filtered_retrieved_data)


def test_insert_multikey_int_str(nhl_data_source):
    data = pd.DataFrame({
        'game_id': [2016020045, 2017020812, 2015020314],
        'venue': ['United Center', 'KeyBank Center', 'foo bar'],
        'home_goals': [0, 1, 2]
    })
    nhl_data_source.insert(
        key=['game_id', 'venue'],
        columns=['home_goals'],
        data=data,
        if_exists='replace'
    )
    retrieved_data = nhl_data_source.select(['game_id', 'venue', 'home_goals'])
    filtered_retrieved_data = retrieved_data[retrieved_data['game_id'].isin([2016020045, 2017020812, 2015020314])]
    expected_filtered_retrieved_data = pd.DataFrame([
        {'game_id': 2016020045, 'venue': 'United Center', 'home_goals': 0},
        {'game_id': 2017020812, 'venue': 'KeyBank Center', 'home_goals': 1},
        {'game_id': 2015020314, 'venue': 'MTS Centre', 'home_goals': 1},
        {'game_id': 2015020314, 'venue': 'foo bar', 'home_goals': 2}
    ])
    assert frames_equal_up_to_row_ordering(filtered_retrieved_data, expected_filtered_retrieved_data)


def test_insert_existing_data_error(users_with_nas_data_source):
    with pytest.raises(ValueError) as exception_info:
        data = pd.DataFrame({'id': [4], 'first_name': [None]})
        users_with_nas_data_source.insert(key=['id'], columns=['first_name'], data=data, if_exists='error')


def test_insert_existing_data_ignore(users_with_nas_data_source):
    data = pd.DataFrame({'id': [4], 'first_name': [None]})
    users_with_nas_data_source.insert(key=['id'], columns=['first_name'], data=data, if_exists='ignore')
    retrieved_data = users_with_nas_data_source.select(['id', 'first_name'])

    filtered_retrieved_data = retrieved_data[retrieved_data['id'] == 4]
    assert str(filtered_retrieved_data['first_name'].iloc[0]) == 'Germaine'


def test_select_from_datasource_with_field_mapping(users_with_nas_field_mapping_data_source):
    data = users_with_nas_field_mapping_data_source.select(['time_stamp', Feature('issubscribed', Bool)])
    assert list(data.columns) == ['time_stamp', 'issubscribed']


def test_insert_to_datasource_with_field_mapping(users_with_nas_field_mapping_data_source):
    data = pd.DataFrame({
        'id': [10, 11],
        'time_stamp': ['2030-08-17T10:41:25', '2030-08-16T15:45:31'],
        'issubscribed': [True, True]
    })
    users_with_nas_field_mapping_data_source.insert(
        key=[Entity('user', join_keys=['id'])],
        columns=[Feature('time_stamp', Timestamp), Feature('issubscribed', Bool)],
        data=data,
        if_exists='replace'
    )
    retrieved_data = users_with_nas_field_mapping_data_source.select(
        ['id', 'time_stamp', Feature('issubscribed', Bool)])
    retrieved_data = retrieved_data[retrieved_data['id'].isin([10, 11])]
    retrieved_data.sort_values(by='id', inplace=True)
    assert_frame_equal(data.reset_index(drop=True), retrieved_data.reset_index(drop=True), check_dtype=False)


def test_insert_into_empty_datasource(empty_data_source):
    data = pd.DataFrame({
        'id': [1, 2, 3],
        'first_name': ['John', 'Jane', 'Mary'],
        'last_name': ['Doe', 'Doe', 'Doe']
    })
    empty_data_source.insert(
        key=['id'],
        columns=['first_name', 'last_name'],
        data=data,
        if_exists='replace'
    )
    retrieved_data = empty_data_source.select(['id', 'first_name', 'last_name'])
    assert frames_equal_up_to_row_ordering(data, retrieved_data)
