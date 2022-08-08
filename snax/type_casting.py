"""Utilities for casting snax.ValueType to pandas.dtypes"""
import json
import re
from datetime import datetime
from typing import List, Union, Any, Optional

import pandas as pd

from snax.feature import Feature
from snax.value_type import ValueType, Null, TimestampList, BoolList, FloatList, IntList, StringList, Timestamp, Bool, \
    Float, Int, String, Unknown


def safe_cast(cast_fn):
    def safe_cast_fn(value, *args, **kwargs):
        if pd.isna(value):
            return None
        try:
            return cast_fn(value, *args, **kwargs)
        except:
            return None

    return safe_cast_fn


@safe_cast
def _cast_unknown(value: Any) -> Any:
    return value


@safe_cast
def _cast_string(value: Any) -> str:
    return str(value)


@safe_cast
def _cast_int(value: Any) -> Union[float, int]:
    return int(float(value))


@safe_cast
def _cast_float(value: Any) -> float:
    return float(value)


@safe_cast
def _cast_bool(value: Any) -> bool:
    if isinstance(value, str):
        if value.lower() in ['true', 't', '1', '1.0']:
            return True
        if value.lower() in ['false', 'f', '0', '0.0']:
            return False

    return bool(value)


@safe_cast
def _cast_timestamp(value: Any, format_string: Optional[str] = None) -> datetime:
    if isinstance(value, str):
        return datetime.strptime(value, format_string)
    elif isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    else:
        raise ValueError(f'Cannot cast {value} to timestamp')


def _strlist_to_list(value: str) -> List[str]:
    return json.loads(value)


@safe_cast
def _cast_string_list(value: Any) -> List[str]:
    raw_values = _strlist_to_list(value)
    return [_cast_string(value) for value in raw_values]


@safe_cast
def _cast_int_list(value: Any) -> List[Union[float, int]]:
    raw_values = _strlist_to_list(value)
    return [_cast_int(value) for value in raw_values]


@safe_cast
def _cast_float_list(value: Any) -> List[float]:
    raw_values = _strlist_to_list(value)
    return [_cast_float(value) for value in raw_values]


@safe_cast
def _cast_bool_list(value: Any) -> List[bool]:
    raw_values = _strlist_to_list(value)
    return [_cast_bool(value) for value in raw_values]


@safe_cast
def _cast_timestamp_list(value: Any, format_string: str) -> List[datetime]:
    raw_values = _strlist_to_list(value)
    return [_cast_timestamp(value, format_string=format_string) for value in raw_values]


@safe_cast
def _cast_null(value: Any) -> Any:
    return value


_FEATURE_TYPE_TO_CAST_FN = {
    Unknown: _cast_unknown,
    String: _cast_string,
    Int: _cast_int,
    Float: _cast_float,
    Bool: _cast_bool,
    Timestamp: _cast_timestamp,
    StringList: _cast_string_list,
    IntList: _cast_int_list,
    FloatList: _cast_float_list,
    BoolList: _cast_bool_list,
    TimestampList: _cast_timestamp_list,
    Null: _cast_null,
}


def _get_first_non_missing_item(series: pd.Series) -> Any:
    for item in series:
        if not pd.isna(item):
            return item

    return None


def _is_string_type(series: pd.Series) -> bool:
    first_nonmissing_item = _get_first_non_missing_item(series)
    return isinstance(first_nonmissing_item, str)


def guess_timestamp_format(series: pd.Series) -> str:
    timestamp_format = _get_first_non_missing_item(series)

    if timestamp_format.strip().startswith('['):
        timestamp_format = _strlist_to_list(timestamp_format)[0]

    time_patterns = [
        (re.compile('[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]+'), '%H:%M:%S.%f'),
        (re.compile('[0-9]{2}:[0-9]{2}:[0-9]{2}'), '%H:%M:%S'),
    ]

    for time_pattern_regex, time_format in time_patterns:
        timestamp_format = time_pattern_regex.sub(time_format, timestamp_format)

    date_patterns = [
        (re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}'), '%Y-%m-%d'),
        (re.compile('[0-9]{2}.[0-9]{2}.[0-9]{4}'), '%d.%m.%Y'),

    ]
    for date_pattern_regex, date_format in date_patterns:
        timestamp_format = date_pattern_regex.sub(date_format, timestamp_format)

    return timestamp_format


def cast_to_feature_type(series: pd.Series, feature_type: ValueType) -> pd.Series:
    cast_fn = _FEATURE_TYPE_TO_CAST_FN.get(feature_type, _cast_unknown)
    kwargs = dict()

    if feature_type in [TimestampList, Timestamp] and _is_string_type(series):
        kwargs['format_string'] = guess_timestamp_format(series)

    def cast_fn_kwargs(value):
        return cast_fn(value, **kwargs)

    return series.apply(cast_fn_kwargs)


def cast_to_feature_types(dataframe: pd.DataFrame, features: List[Feature]) -> pd.DataFrame:
    dataframe = dataframe.copy()
    for feature in features:
        dataframe[feature.name] = cast_to_feature_type(dataframe[feature.name], feature.dtype)

    return dataframe
