from abc import ABC
from typing import Optional, Dict


class DataSource(ABC):
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

    # TODO: Finish implementation
