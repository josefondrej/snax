from datetime import datetime

import pandas as pd
import pytest

from snax.feature import Feature
from snax.type_casting import guess_timestamp_format, cast_to_feature_types, cast_to_feature_type
from snax.value_type import ValueType


def test_cast_to_feature_types():
    data = pd.DataFrame({
        'timestamp': ['2020-01-01', '2020-01-02', '2020-01-03'],
        'bool': ['true', 'false', 'true'],
        'float': ['1.0', '2.0', '3.0'],
        'int': ['1', '2', '3'],
        'string': ['a', 'b', 'c']
    })
    cast_data = cast_to_feature_types(
        dataframe=data,
        features=[
            Feature(name='timestamp', dtype=ValueType.TIMESTAMP),
            Feature(name='bool', dtype=ValueType.BOOL),
            Feature(name='float', dtype=ValueType.FLOAT),
            Feature(name='int', dtype=ValueType.INT),
            Feature(name='string', dtype=ValueType.STRING)
        ]
    )

    assert cast_data['timestamp'].iloc[0] == datetime(2020, 1, 1)
    assert cast_data['bool'].iloc[0] == True
    assert cast_data['float'].iloc[0] == 1.0
    assert cast_data['int'].iloc[0] == 1
    assert cast_data['string'].iloc[0] == 'a'


def test_guess_timestamp_format():
    assert guess_timestamp_format(pd.Series(['2020-01-01', '2020-01-02', '2020-01-03'])) == '%Y-%m-%d'
    assert guess_timestamp_format(pd.Series(['01.01.2020', '01.02.2020', '01.03.2020'])) == '%d.%m.%Y'
    assert guess_timestamp_format(pd.Series(['01:01:17', '01:01:18', '01:01:19'])) == '%H:%M:%S'
    assert guess_timestamp_format(pd.Series(['01:01:17.123', '01:01:18.123', '01:01:19.123'])) == '%H:%M:%S.%f'
    assert guess_timestamp_format(
        pd.Series(['2020-01-02 01:01:17', '2020-01-02 01:01:18', '2020-01-02 01:01:19'])) == '%Y-%m-%d %H:%M:%S'
    assert guess_timestamp_format(pd.Series(
        ['2020-12-02T01:01:17.123', '2020-12-02T01:01:18.123', '2020-12-02T01:01:19.123'])) == '%Y-%m-%dT%H:%M:%S.%f'


RAW_SERIES__FEATURE_TYPE__CAST_SERIES = [
    (
        pd.Series(['2020-01-01', '2020-01-02', None]),
        ValueType.TIMESTAMP,
        pd.Series([datetime(2020, 1, 1), datetime(2020, 1, 2), None])
    ),
    (
        pd.Series(['2020-01-01T01:01:17', '2020-01-02T01:01:18', None]),
        ValueType.TIMESTAMP,
        pd.Series([datetime(2020, 1, 1, 1, 1, 17), datetime(2020, 1, 2, 1, 1, 18), None])
    ),
    (
        pd.Series(['True', 'F', '0.0', None]),
        ValueType.BOOL,
        pd.Series([True, False, False, None])
    ),
    (
        pd.Series(['1.0', '2.0', '3.0', None]),
        ValueType.FLOAT,
        pd.Series([1.0, 2.0, 3.0, None])
    ),
    (
        pd.Series(['1', '2', '3', None]),
        ValueType.INT,
        pd.Series([1, 2, 3, None])
    ),
    (
        pd.Series(['a', 'b', 'c', None]),
        ValueType.STRING,
        pd.Series(['a', 'b', 'c', None])
    ),
    (
        pd.Series([
            '["2020-01-03H15:24:35", "2022-01-04H15:26:35", "2020-01-05H07:24:16", null]',
            '["2023-01-03H15:24:35", "2024-01-04H15:26:35"]',
        ]),
        ValueType.TIMESTAMP_LIST,
        pd.Series([
            [datetime(2020, 1, 3, 15, 24, 35), datetime(2022, 1, 4, 15, 26, 35), datetime(2020, 1, 5, 7, 24, 16), None],
            [datetime(2023, 1, 3, 15, 24, 35), datetime(2024, 1, 4, 15, 26, 35)]
        ])
    ),
    (
        pd.Series([
            '[true, false, true]',
            '[false, null, false]',
            '[true, false, null]'
        ]),
        ValueType.BOOL_LIST,
        pd.Series([
            [True, False, True],
            [False, None, False],
            [True, False, None]
        ])
    ),
    (
        pd.Series([
            '[1.0, 2.0, 3.0]',
            '[2.0, null, 4.0]',
            '[3.0, 4.0, null]'
        ]),
        ValueType.FLOAT_LIST,
        pd.Series([
            [1.0, 2.0, 3.0],
            [2.0, None, 4.0],
            [3.0, 4.0, None]
        ])
    ),
    (
        pd.Series([
            '[1, 2, 3]',
            '[2, 3, null]',
            '[3, null, 5]']
        ),
        ValueType.INT_LIST,
        pd.Series([
            [1, 2, 3],
            [2, 3, None],
            [3, None, 5]
        ])
    ),
    (
        pd.Series([
            '["a", "b", "c"]',
            '["b", "c", null]',
            '["c", "d", "e"]'
        ]),
        ValueType.STRING_LIST,
        pd.Series([
            ['a', 'b', 'c'],
            ['b', 'c', None],
            ['c', 'd', 'e']
        ])
    ),
]


@pytest.mark.parametrize('raw_series,feature_type,expected_cast_series', RAW_SERIES__FEATURE_TYPE__CAST_SERIES)
def test_cast_to_feature_type(raw_series, feature_type, expected_cast_series):
    cast_series = cast_to_feature_type(raw_series, feature_type=feature_type)
    assert cast_series.equals(expected_cast_series)
