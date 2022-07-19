from typing import List, Dict, Optional


class Entity:
    """
    Entity representing some unique object related to our data, e.g. a user, a product, a review, etc.

    Args:
        name: Name of the entity
        join_keys: List of feature names that uniquely identify this entity
        tags: Tags for the entity
    """

    def __init__(self, name: str, join_keys: List[str], tags: Optional[Dict[str, str]] = None):
        self._name = name
        self._join_keys = join_keys
        self._tags = tags or dict()

    def __repr__(self):
        return f'Entity(name={self.name}, join_keys={self.join_keys})'

    def __eq__(self, other):
        if not isinstance(other, Entity):
            return False

        if (self.name != other.name) or (self.join_keys != other.join_keys):
            return False

        return True

    def __hash__(self):
        return hash((self.name, self.join_keys))

    @property
    def name(self) -> str:
        return self._name

    @property
    def join_keys(self) -> List[str]:
        return self._join_keys

    @property
    def tags(self) -> Dict[str, str]:
        return self._tags
