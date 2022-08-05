import os

import pandas as pd
import pytest
from numpy import dtype
from sqlalchemy import create_engine, types, Table, Boolean, Float, Integer, String
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DatabaseError

from snax.data_sources._oracle_utils import ensure_table_exists, drop_table, get_sqlalchemy_table, get_colnames, \
    get_base_column_types, add_unique_constraint, add_columns, get_data_subset_in_db, \
    sqlalchemy_column_type_to_base_type, \
    retype_dataframe, pd_series_to_comma_separated_tuple, escape_value, pd_dataframe_to_comma_separated_tuples, upsert
from snax.utils import frames_equal_up_to_row_ordering

ORACLE_CONNECTION_STRING = os.environ.get('ORACLE_CONNECTION_STRING')
ORACLE_SCHEMA = os.environ.get('ORACLE_SCHEMA')
SAMPLE_DATA_TABLE = 'sample_data'


def insert_test_data(engine: Engine) -> Engine:
    data = pd.DataFrame({
        'id': [1, 2, 3],
        'first_name': ['John', 'Jane', 'Mary'],
        'age': [30, 25, 40],
        'temperature': [98.6, 97.8, 98.3],
        'date_start': ['2020-01-01', '2020-01-02', '2020-01-03'],
        'time_start': ['12:00:00', '12:00:00', '12:00:00'],
        'is_active': [True, False, True],
        'datetime_start': ['2020-01-01 12:00:00', '2020-01-02 12:00:00', '2020-01-03 12:00:00'],
    })
    data.to_sql(
        con=engine, schema=ORACLE_SCHEMA, name=SAMPLE_DATA_TABLE, if_exists='replace', index=False,
        dtype={
            'first_name': types.VARCHAR(100),
            'age': types.INTEGER(),
            'temperature': types.FLOAT(),
            'date_start': types.VARCHAR(30),
            'time_start': types.VARCHAR(30),
            'datetime_start': types.VARCHAR(30)
        }
    )
    return engine


def get_engine() -> Engine:
    if ORACLE_CONNECTION_STRING is None:
        return None

    engine = create_engine(ORACLE_CONNECTION_STRING)
    drop_table(table=SAMPLE_DATA_TABLE, schema=ORACLE_SCHEMA, engine=engine)

    return engine


@pytest.fixture
def empty_engine() -> Engine:  # TODO: Clean-up after use
    engine = get_engine()
    if engine is None:
        return pytest.skip('ORACLE_CONNECTION_STRING is not set')
    return engine


@pytest.fixture
def engine():  # TODO: Clean-up after use
    engine = get_engine()
    if engine is None:
        return pytest.skip('ORACLE_CONNECTION_STRING is not set')

    insert_test_data(engine)
    return engine


def test_ensure_table_exists(empty_engine):
    ensure_table_exists(SAMPLE_DATA_TABLE, ORACLE_SCHEMA, empty_engine)
    result = empty_engine.execute(f'select * from {ORACLE_SCHEMA}.{SAMPLE_DATA_TABLE}')
    assert result.fetchall() == []


def test_drop_table(engine):
    drop_table(SAMPLE_DATA_TABLE, ORACLE_SCHEMA, engine)
    with pytest.raises(DatabaseError):
        engine.execute(f'select * from {ORACLE_SCHEMA}.{SAMPLE_DATA_TABLE}')


def test_get_sqlalchemy_table(engine):
    table = get_sqlalchemy_table(SAMPLE_DATA_TABLE, ORACLE_SCHEMA, engine)
    assert type(table) == Table
    assert table.name == SAMPLE_DATA_TABLE
    assert table.schema == ORACLE_SCHEMA
    assert table.columns[0].name == 'id'


def test_get_colnames(engine):
    colnames = get_colnames(SAMPLE_DATA_TABLE, ORACLE_SCHEMA, engine)
    assert colnames == ['id', 'first_name', 'age', 'temperature', 'date_start', 'time_start', 'is_active',
                        'datetime_start']


def test_get_base_column_types(engine):
    column_types = get_base_column_types(SAMPLE_DATA_TABLE, ORACLE_SCHEMA, engine)
    assert column_types == {
        'id': int,
        'first_name': str,
        'age': int,
        'temperature': float,
        'date_start': str,
        'time_start': str,
        'is_active': int,
        'datetime_start': str,
    }


def test_add_unique_constraint(engine):
    add_unique_constraint(['id', 'first_name'], SAMPLE_DATA_TABLE, ORACLE_SCHEMA, engine)
    sql = f"SELECT * FROM user_cons_columns where CONSTRAINT_NAME = '{ORACLE_SCHEMA.upper()}_{SAMPLE_DATA_TABLE.upper()}_ID_FIRST_NAME_UNIQUE'"
    result = engine.execute(sql)
    expected_result = [
        (ORACLE_SCHEMA.upper(),
         f'{ORACLE_SCHEMA.upper()}_{SAMPLE_DATA_TABLE.upper()}_ID_FIRST_NAME_UNIQUE',
         'SAMPLE_DATA', 'ID', 1),
        (ORACLE_SCHEMA.upper(),
         f'{ORACLE_SCHEMA.upper()}_{SAMPLE_DATA_TABLE.upper()}_ID_FIRST_NAME_UNIQUE',
         'SAMPLE_DATA',
         'FIRST_NAME', 2)]
    assert set(result) == set(expected_result)


def test_add_columns(engine):
    data = pd.DataFrame({
        'middle_name': ['Carlos', 'Ariane', 'Debbie'],
        'height': [1.75, 1.60, 1.80],
        'weight': [70, 65, 80],
    })
    add_columns(
        columns=['middle_name', 'height', 'weight'], data=data,
        table=SAMPLE_DATA_TABLE, schema=ORACLE_SCHEMA, engine=engine
    )
    data = pd.read_sql(f'select * from {ORACLE_SCHEMA}.{SAMPLE_DATA_TABLE}', engine)

    assert 'middle_name' in data.columns
    assert 'height' in data.columns
    assert 'weight' in data.columns


def test_upsert_with_adding_new_rows(engine):
    new_data = pd.DataFrame({
        'id': [4, 5, 6],
        'first_name': ['John', 'Jane', 'Mary'],
        'age': [30, 25, 40],
        'temperature': [98.6, 97.8, 98.3],
        'date_start': ['2020-01-01', '2020-01-02', '2020-01-03'],
        'time_start': ['12:00:00', '12:00:00', '12:00:00'],
        'is_active': [True, False, True],
        'datetime_start': ['2020-01-01 12:00:00', '2020-01-02 12:00:00', '2020-01-03 12:00:00'],
    })

    upsert(
        key=['id', 'first_name'],
        columns=['age', 'temperature', 'date_start', 'time_start', 'is_active', 'datetime_start'],
        data=new_data, table=SAMPLE_DATA_TABLE, schema=ORACLE_SCHEMA, engine=engine
    )

    altered_data = pd.read_sql(sql=f'SELECT * FROM {ORACLE_SCHEMA}.{SAMPLE_DATA_TABLE}', con=engine)

    expected_altered_data = pd.DataFrame(
        {'id': [1, 2, 3, 6, 4, 5],
         'first_name': ['John', 'Jane', 'Mary', 'Mary', 'John', 'Jane'],
         'age': [30, 25, 40, 40, 30, 25],
         'temperature': [98.6, 97.8, 98.3, 98.3, 98.6, 97.8],
         'date_start': ['2020-01-01', '2020-01-02', '2020-01-03', '2020-01-03', '2020-01-01', '2020-01-02'],
         'time_start': ['12:00:00', '12:00:00', '12:00:00', '12:00:00', '12:00:00', '12:00:00'],
         'is_active': [1, 0, 1, 1, 1, 0],
         'datetime_start': [
             '2020-01-01 12:00:00', '2020-01-02 12:00:00', '2020-01-03 12:00:00', '2020-01-03 12:00:00',
             '2020-01-01 12:00:00', '2020-01-02 12:00:00']})

    assert frames_equal_up_to_row_ordering(altered_data, expected_altered_data)


def test_upsert_with_adding_new_rows_with_missings(engine):
    new_data = pd.DataFrame({
        'id': [4, 5, 6],
        'first_name': ['John', 'Jane', 'Mary'],
        'age': [30, 25, 40],
    })

    upsert(
        key=['id', 'first_name'],
        columns=['age'],
        data=new_data, table=SAMPLE_DATA_TABLE, schema=ORACLE_SCHEMA, engine=engine
    )

    altered_data = pd.read_sql(sql=f'SELECT * FROM {ORACLE_SCHEMA}.{SAMPLE_DATA_TABLE}', con=engine)

    expected_altered_data = pd.DataFrame(
        {'id': [1, 2, 3, 6, 4, 5],
         'first_name': ['John', 'Jane', 'Mary', 'Mary', 'John', 'Jane'],
         'age': [30, 25, 40, 40, 30, 25],
         'temperature': [98.6, 97.8, 98.3, None, None, None],
         'date_start': ['2020-01-01', '2020-01-02', '2020-01-03', None, None, None],
         'time_start': ['12:00:00', '12:00:00', '12:00:00', None, None, None],
         'is_active': [1.0, 0.0, 1.0, None, None, None],
         'datetime_start': ['2020-01-01 12:00:00', '2020-01-02 12:00:00', '2020-01-03 12:00:00', None, None, None]}
    )

    assert frames_equal_up_to_row_ordering(altered_data, expected_altered_data)


def test_upsert_replacing_old_rows(engine):
    new_data = pd.DataFrame({
        'id': [1, 2, 3],
        'first_name': ['John', 'Jane', 'Mary'],
        'is_active': [True, True, True],
        'temperature': [98.6, 97.8, 98.3],
    })

    upsert(
        key=['id', 'first_name'],
        columns=['temperature', 'is_active'],
        data=new_data, table=SAMPLE_DATA_TABLE, schema=ORACLE_SCHEMA, engine=engine
    )

    altered_data = pd.read_sql(sql=f'SELECT * FROM {ORACLE_SCHEMA}.{SAMPLE_DATA_TABLE}', con=engine)
    expected_altered_data = pd.DataFrame({
        'id': [1, 2, 3],
        'first_name': ['John', 'Jane', 'Mary'],
        'age': [30, 25, 40],
        'temperature': [98.6, 97.8, 98.3],
        'date_start': ['2020-01-01', '2020-01-02', '2020-01-03'],
        'time_start': ['12:00:00', '12:00:00', '12:00:00'],
        'is_active': [1, 1, 1],
        'datetime_start': ['2020-01-01 12:00:00', '2020-01-02 12:00:00', '2020-01-03 12:00:00']
    })

    assert frames_equal_up_to_row_ordering(altered_data, expected_altered_data)


def test_upsert_inserting_new_columns(engine):
    new_data = pd.DataFrame({
        'id': [1, 2],
        'first_name': ['John', 'Jane'],
        'foobar': [1, 2],
        'barfoo': [3, 4]
    })

    add_columns(['foobar', 'barfoo'], new_data, table=SAMPLE_DATA_TABLE, schema=ORACLE_SCHEMA, engine=engine)
    upsert(
        key=['id', 'first_name'],
        columns=['foobar', 'barfoo'],
        data=new_data, table=SAMPLE_DATA_TABLE, schema=ORACLE_SCHEMA, engine=engine
    )

    altered_data = pd.read_sql(sql=f'SELECT * FROM {ORACLE_SCHEMA}.{SAMPLE_DATA_TABLE}', con=engine)
    expected_altered_data = pd.DataFrame({
        'id': [1, 2, 3],
        'first_name': ['John', 'Jane', 'Mary'],
        'age': [30, 25, 40],
        'temperature': [98.6, 97.8, 98.3],
        'date_start': ['2020-01-01', '2020-01-02', '2020-01-03'],
        'time_start': ['12:00:00', '12:00:00', '12:00:00'], 'is_active': [1, 0, 1],
        'datetime_start': ['2020-01-01 12:00:00', '2020-01-02 12:00:00', '2020-01-03 12:00:00'],
        'foobar': ['1', '2', None],
        'barfoo': ['3', '4', None]
    })

    assert frames_equal_up_to_row_ordering(altered_data, expected_altered_data)


def test_upsert_single_id_column(engine):
    new_data = pd.DataFrame({
        'id': [1, 2, 5],
        'first_name': ['John', 'Jane', 'Mary']
    })

    upsert(
        key=['id'],
        columns=['first_name'],
        data=new_data, table=SAMPLE_DATA_TABLE, schema=ORACLE_SCHEMA, engine=engine
    )

    altered_data = pd.read_sql(sql=f'SELECT * FROM {ORACLE_SCHEMA}.{SAMPLE_DATA_TABLE}', con=engine)
    expected_altered_data = pd.DataFrame({
        'id': [1, 2, 3, 5],
        'first_name': ['John', 'Jane', 'Mary', 'Mary'],
        'age': [30.0, 25.0, 40.0, None],
        'temperature': [98.6, 97.8, 98.3, None],
        'date_start': ['2020-01-01', '2020-01-02', '2020-01-03', None],
        'time_start': ['12:00:00', '12:00:00', '12:00:00', None],
        'is_active': [1.0, 0.0, 1.0, None],
        'datetime_start': ['2020-01-01 12:00:00', '2020-01-02 12:00:00', '2020-01-03 12:00:00', None]
    })
    assert frames_equal_up_to_row_ordering(altered_data, expected_altered_data)


def test_get_data_subset_in_db(engine):
    data = pd.DataFrame({
        'id': [1, 2, 3],
        'first_name': ['John', 'Jane', 'Jack'],
    })
    data_subset = get_data_subset_in_db(data, ['id', 'first_name'], SAMPLE_DATA_TABLE, ORACLE_SCHEMA, engine)
    expected_data_subset = pd.DataFrame({
        'id': [1, 2],
        'first_name': ['John', 'Jane']
    })
    assert data_subset.equals(expected_data_subset)


def test_sqlalchemy_column_type_to_base_type():
    assert sqlalchemy_column_type_to_base_type(String()) == str
    assert sqlalchemy_column_type_to_base_type(Integer()) == int
    assert sqlalchemy_column_type_to_base_type(Float()) == float
    assert sqlalchemy_column_type_to_base_type(Boolean()) == int


def test_retype_dataframe():
    colname_to_type = {'a': str, 'b': int, 'c': float}
    data = pd.DataFrame({
        'a': ['a', 'b', 'c'],
        'b': ['1', '2', '3'],
        'c': ['1.0', '2.0', '3.0']
    })
    retyped_data = retype_dataframe(colname_to_type, data)
    assert retyped_data.dtypes.to_dict() == {'a': dtype('O'), 'b': dtype('int64'), 'c': dtype('float64')}


def test_escape_value():
    assert escape_value(True) == '1'
    assert escape_value(False) == '0'
    assert escape_value(1) == '1'
    assert escape_value(0) == '0'
    assert escape_value(1.0) == '1.0'
    assert escape_value(0.0) == '0.0'
    assert escape_value('abc') == "'abc'"
    assert escape_value('') == "''"
    assert escape_value(None) == 'null'


def test_pd_series_to_comma_separated_tuple():
    series = pd.Series({'a': 1, 'b': 5.6, 'c': True, 'd': 'str'})
    assert pd_series_to_comma_separated_tuple(series) == "(1, 5.6, 1, 'str')"


def test_pd_dataframe_to_comma_separated_tuples():
    data = pd.DataFrame({
        'a': [1, 2, 3],
        'b': [5.6, 7.8, 9.0],
        'c': [True, False, True],
        'd': ['str', 'str2', 'str3']
    })
    assert pd_dataframe_to_comma_separated_tuples(data) == \
           "(1, 5.6, 1, 'str'), (2, 7.8, 0, 'str2'), (3, 9.0, 1, 'str3')"
