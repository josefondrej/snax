import logging
from typing import Optional, Dict, List

import pandas as pd
from sqlalchemy.engine import Engine

from snax.data_sources.data_source_base import DataSourceBase

logger = logging.getLogger(__name__)


class OracleDataSource(DataSourceBase):
    def __init__(self, name: str, schema: str, table: str, engine: Engine,
                 field_mapping: Optional[Dict[str, str]] = None, tags: Optional[Dict] = None):
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
        super().__init__(name=name, field_mapping=field_mapping, tags=tags)
        self._engine = engine
        self._schema = schema
        self._table = table

        self._ensure_table_exists()

    def _select(self, columns: Optional[List[str]] = None, where_sql_query: Optional[str] = None) -> pd.DataFrame:
        raise NotImplementedError('TODO: Implement')

    def _insert(self, key: List[str], columns: List[str], data: pd.DataFrame, if_exists: str = 'error'):
        raise NotImplementedError('TODO: Implement')

    def _ensure_table_exists(self):
        """Checks if schema.table exists in the Oracle DB and creates it if it doesn't"""
        query = f'SELECT * FROM {self._schema}.{self._table}'
        try:
            pd.read_sql(query, self._engine, chunksize=1)
            logger.info(f'Table {self._schema}.{self._table} exists')
        except Exception:
            query = f'CREATE TABLE {self._schema}.{self._table} (dummy int)'
            self._engine.execute(query)
            logger.info(f'Table {self._schema}.{self._table} created')

    def delete(self):
        query = f'DROP TABLE {self._schema}.{self._table}'
        self._engine.execute(query)
