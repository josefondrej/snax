import os
from typing import Optional, Dict, List

import pandas as pd

from snax.data_sources.in_memory_data_source import InMemoryDataSource


class CsvDataSource(InMemoryDataSource):
    def __init__(self, name: str, csv_file_path: str, separator: str = ',',
                 field_mapping: Optional[Dict[str, str]] = None, tags: Optional[Dict] = None):
        super().__init__(name=name, data=None, field_mapping=field_mapping, tags=tags)
        self._csv_file_path = csv_file_path
        self._separator = separator

    @property
    def csv_file_path(self) -> str:
        return self._csv_file_path

    @property
    def separator(self) -> str:
        return self._separator

    def _load_data(self) -> pd.DataFrame:
        if os.path.exists(self._csv_file_path):
            data = pd.read_csv(self.csv_file_path, sep=self.separator)
            self._data = data
        else:
            self._data = pd.DataFrame()

    def _dump_data(self):
        self._data.to_csv(self.csv_file_path, sep=self.separator, index=False)

    def _select(self, columns: Optional[List[str]] = None, where_sql_query: Optional[str] = None) -> pd.DataFrame:
        if self._data is None:
            self._load_data()

        return super()._select(columns=columns, where_sql_query=where_sql_query)

    def _insert(self, key: List[str], columns: List[str], data: pd.DataFrame, if_exists: str = 'error'):
        if self._data is None:
            self._load_data()

        super()._insert(key=key, columns=columns, data=data, if_exists=if_exists)

        self._dump_data()
