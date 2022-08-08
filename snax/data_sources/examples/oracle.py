import logging
import os

import pandas as pd
import sqlalchemy.types
from sqlalchemy import create_engine

from snax.data_sources._oracle_utils import drop_table
from snax.data_sources.oracle_data_source import OracleDataSource
from snax.example_feature_repos.sports_feature_repo.nhl_games import data_path as original_nhl_data_path
from snax.example_feature_repos.users_with_nas_feature_repo.users_with_nas import \
    data_path as original_users_with_na_data_path

_ORACLE_CONNECTION_STRING = os.environ.get('ORACLE_CONNECTION_STRING')
_ORACLE_SCHEMA = os.environ.get('ORACLE_SCHEMA')
_ORACLE_NHL_TABLE = 'nhl_games'
_ORACLE_USERS_WITH_NAS_TABLE = 'users_with_nas'

_NHL_DATA = pd.read_csv(original_nhl_data_path)
_USERS_WITH_NA_DATA = pd.read_csv(original_users_with_na_data_path)

logger = logging.getLogger(__name__)


def none_if_oracle_not_set(func):
    def wrapper(*args, **kwargs):
        if _ORACLE_CONNECTION_STRING is None:
            return None
        return func(*args, **kwargs)

    return wrapper


@none_if_oracle_not_set
def create_nhl_games() -> OracleDataSource:
    engine = create_engine(_ORACLE_CONNECTION_STRING)
    _NHL_DATA.to_sql(
        con=engine, schema=_ORACLE_SCHEMA, name=_ORACLE_NHL_TABLE, if_exists='replace', index=False,
        dtype={'venue': sqlalchemy.types.VARCHAR(100)})  # Change to VARCHAR because default CLOB can't be index

    return OracleDataSource(
        name='nhl_games_in_memory',
        schema=_ORACLE_SCHEMA,
        table=_ORACLE_NHL_TABLE,
        engine=engine
    )


@none_if_oracle_not_set
def create_users_with_nas() -> OracleDataSource:
    engine = create_engine(_ORACLE_CONNECTION_STRING)
    _USERS_WITH_NA_DATA.to_sql(
        con=engine, schema=_ORACLE_SCHEMA, name=_ORACLE_USERS_WITH_NAS_TABLE, if_exists='replace', index=False,
        dtype={'string_id': sqlalchemy.types.VARCHAR(100)})  # Change to VARCHAR because default CLOB can't be index

    return OracleDataSource(
        name='users_with_nas_in_memory',
        schema=_ORACLE_SCHEMA,
        table=_ORACLE_USERS_WITH_NAS_TABLE,
        engine=engine
    )


@none_if_oracle_not_set
def create_users_with_nas_field_mapping() -> OracleDataSource:
    engine = create_engine(_ORACLE_CONNECTION_STRING)
    _USERS_WITH_NA_DATA.to_sql(
        con=engine, schema=_ORACLE_SCHEMA, name=_ORACLE_USERS_WITH_NAS_TABLE, if_exists='replace', index=False)

    return OracleDataSource(
        name='users_with_nas_in_memory',
        schema=_ORACLE_SCHEMA,
        table=_ORACLE_USERS_WITH_NAS_TABLE,
        engine=engine,
        field_mapping={
            'is_subscribed': 'issubscribed',
            'timestamp': 'time_stamp',
        }
    )


@none_if_oracle_not_set
def create_empty_data_source() -> OracleDataSource:
    engine = create_engine(_ORACLE_CONNECTION_STRING)
    drop_table(table=_ORACLE_NHL_TABLE, schema=_ORACLE_SCHEMA, engine=engine)

    return OracleDataSource(
        name='empty_oracle',
        schema=_ORACLE_SCHEMA,
        table='empty_table',
        engine=engine
    )
