import logging
from typing import Optional, Dict, List

import pandas as pd
from pandas import MultiIndex
from sqlalchemy.engine import Engine

from snax.data_sources._oracle_utils import drop_table, add_columns, upsert, get_data_subset_in_db, \
    add_unique_constraint, ensure_table_exists, get_colnames, ensure_columns_exist, \
    pd_dataframe_to_comma_separated_tuples
from snax.data_sources.data_source_base import DataSourceBase

logger = logging.getLogger(__name__)


class OracleDataSource(DataSourceBase):
    """
    Oracle DB based data source

        Args:
            name: Name of the data source
            engine: SQLAlchemy engine to the Oracle DB
            schema: Name of the schema where the data is located
            table: Name of the table where the data is located
            field_mapping: A mapping from field names in this data source to feature names
            tags: Tags for the data source
    """

    def __init__(self, name: str, schema: str, table: str, engine: Engine,
                 field_mapping: Optional[Dict[str, str]] = None, tags: Optional[Dict] = None):
        super().__init__(name=name, field_mapping=field_mapping, tags=tags)
        self._engine = engine
        self._schema = schema
        self._table = table

        ensure_table_exists(self._table, self._schema, self._engine)

    def _select(self, columns: Optional[List[str]] = None, where_sql_query: Optional[str] = None) -> pd.DataFrame:
        joined_columns = ','.join(columns) if columns else '*'
        query = f'SELECT {joined_columns} FROM {self._schema}.{self._table}'
        if where_sql_query:
            query += f' WHERE {where_sql_query}'

        data = pd.read_sql(query, self._engine)
        return data

    def _insert(self, key: List[str], columns: List[str], data: pd.DataFrame, if_exists: str = 'error'):
        ensure_columns_exist(key, data.dtypes.to_dict(), self._table, self._schema, self._engine)
        add_unique_constraint(key, self._table, self._schema, self._engine)
        data = data.copy()

        existing_key_values = MultiIndex.from_frame(
            get_data_subset_in_db(data, key, self._table, self._schema, self._engine))
        inserted_key_values = MultiIndex.from_frame(data[key])

        inserted_in_existing = [item in existing_key_values for item in inserted_key_values]
        inserted_not_in_existing = [not item for item in inserted_in_existing]
        existing_columns = get_colnames(self._table, self._schema, self._engine)
        new_columns = [colname for colname in list(data.columns) if colname not in existing_columns]

        if if_exists == 'error':
            common_existing_and_inserted_columns = (set(data.columns) - set(key)).intersection(existing_columns)
            if any(inserted_in_existing) and len(common_existing_and_inserted_columns) > 0:
                raise ValueError(f'Data already exists in {self._schema}.{self._table}')

        if len(new_columns) > 0:
            add_columns(new_columns, data, self._table, self._schema, self._engine)
            upsert(key, new_columns, data, self._table, self._schema, self._engine)

        if any(inserted_not_in_existing):
            upsert(key, columns, data[inserted_not_in_existing], self._table, self._schema, self._engine)

        if if_exists == 'replace' and any(inserted_in_existing):
            upsert(key, columns, data[inserted_in_existing], self._table, self._schema, self._engine)

    def _where_sql_query_from_key_values(self, key: List[str], key_values: pd.DataFrame) -> str:
        query = f"({', '.join(key)}) IN ({pd_dataframe_to_comma_separated_tuples(key_values)})"
        return query

    def delete(self):
        drop_table(self._table, self._schema, self._engine)
