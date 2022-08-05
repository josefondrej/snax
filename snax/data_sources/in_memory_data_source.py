from typing import Optional, Dict, List, Any

import pandas as pd

from snax.data_sources.data_source_base import DataSourceBase


def escape(value: Any) -> str:
    if isinstance(value, str):
        return f"'{value}'"
    return value


class InMemoryDataSource(DataSourceBase):
    def __init__(self, name: str, data: Optional[pd.DataFrame] = None, field_mapping: Optional[Dict[str, str]] = None,
                 tags: Optional[Dict] = None):
        super().__init__(name=name, field_mapping=field_mapping, tags=tags)
        self._data = data

    def _select(self, columns: Optional[List[str]] = None, where_sql_query: Optional[str] = None) -> pd.DataFrame:
        data_subset = self._data.copy()
        if where_sql_query is not None:
            data_subset = data_subset.query(where_sql_query)

        if columns is not None:
            return data_subset.loc[:, columns]
        else:
            return data_subset

    def _insert(self, key: List[str], columns: List[str], data: pd.DataFrame, if_exists: str = 'error'):
        self._ensure_key_in_data(key)
        data_to_insert = data[key + columns].copy()

        data_index = pd.MultiIndex.from_frame(self._data[key])
        insert_index = pd.MultiIndex.from_frame(data_to_insert[key])

        data_indices_of_inserted_rows = data_index.get_indexer_for(insert_index)

        if if_exists == 'error':
            existing_data_colnames = list(self._data.columns)
            inserted_data_have_some_original_colnames = any(colname in existing_data_colnames for colname in columns)
            inserted_data_have_some_original_keys = any(data_indices_of_inserted_rows != -1)
            if inserted_data_have_some_original_colnames and inserted_data_have_some_original_keys:
                raise ValueError('Some of the inserted data already exists in the data source')

        new_columns = [colname for colname in data_to_insert.columns if colname not in self._data.columns]
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

    def _where_sql_query_from_key_values(self, key: List[str], key_values: pd.DataFrame) -> str:
        row_to_string = lambda row: '(' + ' and '.join(f'{key_}=={escape(row[key_])}' for key_ in key) + ')'
        query = ' or '.join(list(key_values.apply(row_to_string, axis=1)))
        return query

    def _ensure_key_in_data(self, key: List[str]):
        for key_ in key:
            if key_ not in self._data:
                self._data[key_] = None
