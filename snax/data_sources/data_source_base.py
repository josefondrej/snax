from abc import ABC
from typing import Optional, Dict, List

import pandas as pd

from snax.column_like import ColumnLike, get_features_names

_VALID_IF_EXISTS_OPTIONS = ['error', 'ignore', 'replace']


class DataSourceBase(ABC):
    """
    Representation of a single table of data stored in some database
    It's main responsibility is to provide a data-frame (not necessarily with the correct column types) that represents
    (subset) of the underlying data
    It's secondary responsibility can be to provide a way how to modify the underlying data given a data-frame

    Args:
        name: Name of the data source
        field_mapping: A mapping from field names in this data source to feature names
        tags: Tags for the data source
    """

    def __init__(self, name: str, field_mapping: Optional[Dict[str, str]] = None, tags: Optional[Dict] = None):
        self._name = name
        self._field_mapping = field_mapping or dict()
        self._tags = tags or dict()

    def __repr__(self):
        return f'DataSource(name={self.name})'

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        if (self.name != other.name):
            return False

        return True

    def __hash__(self):
        return hash(self.name)

    @property
    def name(self) -> str:
        return self._name

    @property
    def field_mapping(self) -> Dict[str, str]:
        return self._field_mapping

    @property
    def tags(self) -> Dict:
        return self._tags

    def select(self, columns: Optional[List[ColumnLike]] = None, key: Optional[List[ColumnLike]] = None,
               key_values: Optional[pd.DataFrame] = None, where_sql_query: Optional[str] = None) -> pd.DataFrame:
        """
        Select a subset of the underlying data
        This can be done either by specifying the key and key_values or by specifying the where_sql_query

        Args:
            columns: Entities, Features or feature names to select, if None, all columns are selected
            key: List of column names giving unique constraint on a row in the data source
            key_values: Data frame with the key values
            where_sql_query: Optional filter query to apply to the selection, for now language depends on the data source

        Returns:
            A DataFrame containing the selected data
        """
        if (key is not None or key_values is not None) and where_sql_query is not None:
            raise ValueError('Cannot specify both key, key_values and where_sql_query')
        if (key is None and key_values is not None) or (key is not None and key_values is None):
            raise ValueError('Must specify both key and key_values')

        if key is not None and key_values is not None:
            string_key = self._column_likes_to_colnames(key)
            where_sql_query = self._where_sql_query_from_key_values(string_key, key_values)

        string_columns = self._column_likes_to_colnames(columns)
        selected_data = self._select(string_columns, where_sql_query)
        selected_data.rename(columns=self._field_mapping, inplace=True)
        return selected_data

    def insert(self, key: List[ColumnLike], columns: List[ColumnLike], data: pd.DataFrame, if_exists: str = 'error'):
        """
        Insert feature values corresponding to the given keys into the data source

        Args:
            key: List of column names giving unique constraint on a row in the data source
            columns: List of column names to use in the insert
            data: Data to insert, the column names must correspond to feature names (not to the
                data sources' underlying field names)
            if_exists: What to do if the data source already contains non-empty data for the given keys
                can be one of 'error', 'replace', 'ignore'

        Returns:
            None
        """
        if if_exists not in _VALID_IF_EXISTS_OPTIONS:
            raise ValueError(f'if_exists must be one of {_VALID_IF_EXISTS_OPTIONS}')

        string_key = self._column_likes_to_colnames(key)
        string_columns = self._column_likes_to_colnames(columns)
        self._insert(string_key, string_columns, data.rename(columns=self._inverse_field_mapping), if_exists)

    def _where_sql_query_from_key_values(self, key: List[str], key_values: pd.DataFrame) -> str:
        raise NotImplementedError('Has to be overridden by subclass')

    def _select(self, columns: Optional[List[str]] = None, where_sql_query: Optional[str] = None) -> pd.DataFrame:
        raise NotImplementedError('Has to be overridden by subclass')

    def _insert(self, key: List[str], columns: List[str], data: pd.DataFrame, if_exists: str = 'error'):
        raise NotImplementedError('Has to be overridden by subclass')

    @property
    def _inverse_field_mapping(self) -> Dict:
        if not hasattr(self, '_inverse_field_mapping_') or self._inverse_field_mapping_ is None:
            self._inverse_field_mapping_ = {value: key for key, value in self.field_mapping.items()}
        return self._inverse_field_mapping_

    def _column_likes_to_colnames(self, column_likes: List[ColumnLike]):
        if column_likes is None:
            return None

        feature_names = get_features_names(column_likes)
        column_names = list(map(lambda name: self._inverse_field_mapping.get(name, name), feature_names))
        return column_names
