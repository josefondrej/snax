from typing import Optional, Dict, List

import pandas as pd

from snax.data_sources.data_source import DataSourceBase


class CsvDataSource(DataSourceBase):
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

    def _dump_data(self):
        self.data.to_csv(self.csv_file_path, sep=self.separator, index=False)
        # TODO: Handle renaming of columns
        self._load_data()

    def _select(self, columns: Optional[List[str]] = None, where_sql_query: Optional[str] = None) -> pd.DataFrame:
        data_subset = self.data.copy()
        if where_sql_query is not None:
            data_subset = data_subset.query(where_sql_query)

        if columns is not None:
            return data_subset.loc[:, columns]
        else:
            return data_subset

    def _insert(self, key: List[str], columns: List[str], data: pd.DataFrame, if_exists: str = 'error'):
        data_to_insert = data[key + columns].copy()

        data_index = pd.MultiIndex.from_frame(self.data[key])
        insert_index = pd.MultiIndex.from_frame(data_to_insert[key])

        data_indices_of_inserted_rows = data_index.get_indexer_for(insert_index)

        if if_exists == 'error':
            existing_data_colnames = list(self.data.columns)
            inserted_data_have_some_original_colnames = any(colname in existing_data_colnames for colname in columns)
            inserted_data_have_some_original_keys = any(data_indices_of_inserted_rows != -1)
            if inserted_data_have_some_original_colnames and inserted_data_have_some_original_keys:
                raise ValueError('Some of the inserted data already exists in the data source')

        new_columns = [colname for colname in data_to_insert.columns if colname not in self.data.columns]
        for original_index_of_inserted_row, (_, inserted_row) \
                in zip(data_indices_of_inserted_rows, data_to_insert.iterrows()):
            # TODO: Optimize speed by splitting on indices with -1 and not -1
            if original_index_of_inserted_row == -1:
                self._data = self._data.append(inserted_row)
            else:
                if if_exists == 'replace' or if_exists == 'error':
                    self._data.loc[original_index_of_inserted_row, columns] = inserted_row
                elif if_exists == 'ignore':
                    self._data.loc[original_index_of_inserted_row, new_columns] = new_columns
                else:
                    raise ValueError(f'Unknown if_exists value: {if_exists}')

        self._dump_data()
