import logging
from typing import List, Dict, Optional

import pandas as pd
from sqlalchemy import MetaData, Table, String, Integer, Float, Boolean
from sqlalchemy.engine import Engine
from sqlalchemy.sql.type_api import TypeEngine

logger = logging.getLogger(__name__)


def ensure_table_exists(table: str, schema: str, engine: Engine):
    """Checks if schema.table exists in the Oracle DB and creates it if it doesn't"""
    query = f'SELECT * FROM {schema}.{table}'
    try:
        pd.read_sql(query, engine, chunksize=1)
        logger.info(f'Table {schema}.{table} exists')
    except Exception:
        query = f'CREATE TABLE {schema}.{table} (dummy int)'
        engine.execute(query)
        logger.info(f'Table {schema}.{table} created')


def drop_table(table: str, schema: str, engine: Engine):
    """Drops schema.table from the Oracle DB"""
    query = f'DROP TABLE {schema}.{table}'
    engine.execute(query)
    logger.info(f'Table {schema}.{table} dropped')


def get_sqlalchemy_table(table: str, schema: str, engine: Engine) -> Table:
    return Table(table, MetaData(), autoload_with=engine, schema=schema)


def get_colnames(table: str, schema: str, engine: Engine) -> List[str]:
    """Returns a list of column names for schema.table"""
    table = get_sqlalchemy_table(table, schema, engine)
    return [col.name for col in table.columns]


def get_column_types(table: str, schema: str, engine: Engine) -> Dict[str, Optional[type]]:
    sqlalchemy_table = get_sqlalchemy_table(table, schema, engine)
    colname_to_type = dict()
    for column in sqlalchemy_table.columns:
        column_type = sqlalchemy_column_type_to_base_type(column.type)
        colname_to_type[column.name] = column_type
    return colname_to_type


def add_unique_constraint(key: List[str], table: str, schema: str, engine: Engine):
    constraint_name = '_'.join(key) + '_unique'
    sql = f'ALTER TABLE {schema}.{table} ADD CONSTRAINT {constraint_name} UNIQUE ({", ".join(key)});'
    try:
        engine.execute(sql)
    except Exception as exception:  # TODO: Implement the exception handling
        print(exception)


def add_columns(columns: List[str], data: pd.DataFrame, table: str, schema: str, engine: Engine):
    raise NotImplementedError  # TODO: Implement


def upsert(key: List[str], columns: List[str], data: pd.DataFrame, table: str, schema: str, engine: Engine):
    raise NotImplementedError  # TODO: Implement


def get_data_subset_in_db(data: pd.DataFrame, colnames: List[str], table: str, schema: str,
                          engine: Engine) -> pd.DataFrame:
    """Returns subset of data[colnames] that already exist in the Oracle DB"""
    colnames_separated_by_comma = ', '.join(colnames)
    value_tuples_separated_by_comma = pd_dataframe_to_comma_separated_tuples(data[colnames])
    data_in_db = pd.read_sql(
        sql=f'SELECT {colnames_separated_by_comma} FROM {schema}.{table} ' \
            f'WHERE ({colnames_separated_by_comma}) IN ({value_tuples_separated_by_comma})',
        con=engine
    )
    return data_in_db


# ----------------------------------------------------------------------------------------------------------------------

SQLALCHEMY_TO_PYTHON_TYPE = {
    String: str,
    Integer: int,
    Float: float,
    Boolean: int  # oracle does not have a boolean types
}


def sqlalchemy_column_type_to_base_type(column_type: TypeEngine) -> Optional[type]:
    for sqlalchemy_type, python_type in SQLALCHEMY_TO_PYTHON_TYPE.items():
        if issubclass(column_type.__class__, sqlalchemy_type):
            return python_type


def retype_dataframe(colname_to_type: Dict[str, Optional[type]], data: pd.DataFrame) -> pd.DataFrame:
    data = data.copy()
    for colname, coltype in colname_to_type.items():
        if colname in data and coltype is not None:
            data[colname] = data[colname].astype(coltype)
    return data


def pd_dataframe_to_comma_separated_tuples(data: pd.DataFrame) -> str:
    return ', '.join(list(data.apply(pd_series_to_comma_separated_tuple, axis=1)))


def pd_series_to_comma_separated_tuple(series: pd.Series) -> str:
    comma_joined = ', '.join([f"'{item}'" if isinstance(item, str) else str(item) for item in series])
    return f'({comma_joined})'
