from typing import Dict, Optional

from snax.value_type import ValueType


class Feature:
    """
    A property that we measure / observe

    Args:
        name: Name of the feature
        dtype: Data type of the feature
        tags: Tags for the feature
    """

    def __init__(self, name: str, dtype: ValueType, tags: Optional[Dict[str, str]] = None):
        self._name = name
        self._dtype = dtype
        self._tags = tags or dict()

    def __repr__(self):
        return f'Feature(name={self.name}, dtype={self.dtype})'

    def __eq__(self, other):
        if not isinstance(other, Feature):
            return False

        if (self.name != other.name) or (self.dtype != other.dtype):
            return False

        return True

    def __hash__(self):
        return hash((self.name, self.dtype))

    @property
    def name(self) -> str:
        return self._name

    @property
    def dtype(self) -> ValueType:
        return self._dtype

    @property
    def tags(self) -> Dict[str, str]:
        return self._tags
