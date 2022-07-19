from typing import List, Optional, Dict

from snax.data_source import DataSource
from snax.entity import Entity
from snax.feature import Feature


class FeatureView:
    """
    Group of features and entities from a single data source that create logical group of data

    Args:
        name: Name of the feature view
        entities: List of entities that are part of this feature view
        features: List of features that are part of this feature view
        source: Data source that this feature view is based on
        tags: Tags for this feature view
    """

    def __init__(self, name: str, entities: Optional[List[Entity]], features: Optional[List[Feature]],
                 source: DataSource, tags: Optional[Dict[str, str]] = None):
        self._name = name
        self._entities = entities
        self._features = features
        self._source = source
        self._tags = tags or dict()

    @property
    def name(self) -> str:
        return self._name

    @property
    def entities(self) -> Optional[List[str]]:
        return self._entities

    @property
    def features(self) -> List[Feature]:
        return self._features

    @property
    def source(self) -> DataSource:
        return self._source

    @property
    def tags(self) -> Dict[str, str]:
        return self._tags
