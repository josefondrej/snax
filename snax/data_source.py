from abc import ABC
from typing import Optional, Dict, List

import pandas as pd

from snax.column_like import ColumnLike


class DataSource(ABC):
    # TODO: Finish implementation
    """
    A single table of data stored in some database

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

    def select(self, what: List[ColumnLike], where_sql_query: Optional[str] = None) -> pd.DataFrame:
        """
        Select a subset of the underlying data
        Args:
            what: Entities, Features or feature names to select
            where_sql_query: Optional filter query to apply to the selection, language depends on the data source # TODO: Unify the language so it does not depend on the data source

        Returns:
            A DataFrame containing the selected data
        """
        raise NotImplementedError('Has to be overridden by subclass')

    def insert(self, key: List[ColumnLike], columns: List[ColumnLike], data: pd.DataFrame, if_exists: str = 'error'):
        """
        Insert feature values corresponding to the given keys into the data source

        Args:
            key: List of column names giving unique constraint on a row in the data source
            columns: List of column names to use in the insert
            data: Underlying data to insert
            if_exists: What to do if the data source already contains non-empty data for the given keys
                can be one of 'error', 'replace', 'ignore'

        Returns:
            None
        """
        raise NotImplementedError('Has to be overridden by subclass')
