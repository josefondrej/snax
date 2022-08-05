import logging
from typing import List, Dict, Optional

import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import MetaData, Table, String, Integer, Float, Boolean
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DatabaseError
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
    try:
        engine.execute(query)
        logger.info(f'Table {schema}.{table} dropped')
    except sqlalchemy.exc.DatabaseError as exception:
        if len(exception.args) > 0 and 'ORA-00942' in exception.args[0]:
            logger.debug(f'Can\'t drop table that doesn\'t exist: {exception.args[0]}')
        else:
            raise exception


def get_sqlalchemy_table(table: str, schema: str, engine: Engine) -> Table:
    return Table(table, MetaData(), autoload_with=engine, schema=schema)


def get_colnames(table: str, schema: str, engine: Engine) -> List[str]:
    """Returns a list of column names for schema.table"""
    table = get_sqlalchemy_table(table, schema, engine)
    return [col.name for col in table.columns]


def get_sqlalchemy_column_types(table: str, schema: str, engine: Engine) -> Dict[str, Optional[type]]:
    sqlalchemy_table = get_sqlalchemy_table(table, schema, engine)
    colname_to_type = dict()
    for column in sqlalchemy_table.columns:
        colname_to_type[column.name] = column.type
    return colname_to_type


def get_base_column_types(table: str, schema: str, engine: Engine) -> Dict[str, Optional[type]]:
    sqlalchemy_column_types = get_sqlalchemy_column_types(table, schema, engine)
    return {colname: sqlalchemy_column_type_to_base_type(column_type) for colname, column_type in
            sqlalchemy_column_types.items()}


def add_unique_constraint(key: List[str], table: str, schema: str, engine: Engine):
    constraint_name = schema.upper() + '_' + table.upper() + '_' + '_'.join([k.upper() for k in key]) + '_unique'
    sql = f'ALTER TABLE {schema}.{table} ADD CONSTRAINT {constraint_name} UNIQUE ({", ".join(key)})'
    try:
        engine.execute(sql)
    except DatabaseError as exception:
        if len(exception.args) > 0 and 'ORA-02261' in exception.args[0]:
            logger.debug(f'Unique constraint {constraint_name} already exists')
        else:
            raise exception


def get_oracle_type(column_type: type) -> str:
    if isinstance(column_type, int):
        return 'NUMBER'
    elif isinstance(column_type, float):
        return 'FLOAT'
    elif isinstance(column_type, str):
        return 'VARCHAR2'
    elif isinstance(column_type, bool):
        return 'NUMBER'
    else:
        return 'CLOB'


def add_columns(columns: List[str], data: pd.DataFrame, table: str, schema: str, engine: Engine):
    for column in columns:
        column_type = data[column].dtype
        oracle_type = get_oracle_type(column_type)
        sql = f'ALTER TABLE {schema}.{table} ADD {column} {oracle_type}'
        engine.execute(sql)
        logger.info(f'Added column {column} to table {schema}.{table}')


def upsert(key: List[str], columns: List[str], data: pd.DataFrame, table: str, schema: str, engine: Engine):
    tmp_table = f'{table}_tmp'
    table_column_types = get_sqlalchemy_column_types(table, schema, engine)
    data[key + columns].to_sql(
        tmp_table, engine, if_exists='replace', schema=schema, index=False, dtype=table_column_types)

    condition = ' and '.join([f'{table}.{key_} = {tmp_table}.{key_}' for key_ in key])
    sql = f'merge into {schema}.{table} using {schema}.{tmp_table} on ({condition}) ' \
          f'when matched then update set {", ".join([f"{table}.{column} = {tmp_table}.{column}" for column in columns])} ' \
          f'when not matched then insert ({", ".join(key + columns)}) values ({", ".join([f"{tmp_table}.{column}" for column in (key + columns)])})'

    with engine.begin() as conn:
        # See https://github.com/sqlalchemy/sqlalchemy/issues/5405 for details why we do not use just engine.execute()
        conn.execute(sql)

    drop_table(tmp_table, schema, engine)


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


def escape_value(value: str) -> str:
    if pd.isna(value):
        return 'null'

    if isinstance(value, str):
        return f"'{value}'"
    elif isinstance(value, bool):
        return str(int(value))
    else:
        return str(value)


def pd_series_to_comma_separated_tuple(series: pd.Series) -> str:
    comma_joined = ', '.join([escape_value(item) for item in series])
    return f'({comma_joined})'


numpy_dtype_to_oracle_dict = {
    np.dtype('O'): 'CLOB',
    np.dtype('int32'): 'FLOAT',
    np.dtype('int64'): 'NUMBER',
    np.dtype('float32'): 'FLOAT',
    np.dtype('float64'): 'FLOAT',
    np.dtype('bool'): 'NUMBER'
}


def numpy_dtype_to_oracle(dtype: type) -> str:
    return numpy_dtype_to_oracle_dict.get(dtype, 'CLOB')


def add_column(column: str, dtype: type, table: str, schema: str, engine: Engine):
    oracle_dtype = numpy_dtype_to_oracle(dtype)
    sql = f'alter table {schema}.{table} add {column} {oracle_dtype}'
    engine.execute(sql)


def ensure_columns_exist(columns: List[str], dtypes: Dict[str, type], table: str, schema: str, engine: Engine):
    columns_in_db = get_colnames(table, schema, engine)
    for column in columns:
        if column not in columns_in_db:
            add_column(column, dtypes[column], table, schema, engine)
