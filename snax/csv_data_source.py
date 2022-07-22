from typing import Optional, Dict, List

import pandas as pd

from snax.data_source import DataSource


class CsvDataSource(DataSource):
    # TODO: Finish implementation
    def __init__(self, name: str, csv_file_path: str, separator: str = ',',
                 field_mapping: Optional[Dict[str, str]] = None, tags: Optional[Dict] = None):
        super().__init__(name, field_mapping, tags)
        self._csv_file_path = csv_file_path
        self._separator = separator
        self._data = None

    @property
    def csv_file_path(self) -> str:
        return self._csv_file_path

    @property
    def separator(self) -> str:
        return self._separator

    @property
    def data(self) -> pd.DataFrame:
        if self._data is None:
            self._data = self._load_data()
        return self._data

    def _load_data(self) -> pd.DataFrame:
        data = pd.read_csv(self.csv_file_path, sep=self.separator)
        if self.field_mapping is not None:
            data.rename(columns=self.field_mapping, inplace=True)
        return data

    def _select(self, columns: Optional[List[str]] = None, where_sql_query: Optional[str] = None) -> pd.DataFrame:
        data_subset = self.data.copy()
        if where_sql_query is not None:
            data_subset = data_subset.query(where_sql_query)

        if columns is not None:
            return data_subset.loc[:, columns]
        else:
            return data_subset

    def _insert(self, key: List[str], columns: List[str], data: pd.DataFrame, if_exists: str = 'error'):
        raise NotImplementedError('TODO: Implement')
