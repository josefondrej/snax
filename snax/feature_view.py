from typing import List, Optional, Dict

import pandas as pd

from snax.data_sources.data_source_base import DataSourceBase
from snax.entity import Entity
from snax.feature import Feature
from snax.type_casting import cast_to_feature_types


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
                 source: DataSourceBase, tags: Optional[Dict[str, str]] = None):
        self._name = name
        self._entities = entities
        self._features = features
        self._source = source
        self._tags = tags or dict()

    def __repr__(self):
        return f'FeatureView(name={self.name})'

    def __eq__(self, other):
        if not isinstance(other, FeatureView):
            return False

        if self.name != other.name:
            return False

        if ((self.entities is None) and (other.entities is not None)) or \
                ((self.entities is not None) and (other.entities is None)):
            return False

        if self.entities is not None:
            for entity in self.entities:
                if entity not in other.entities:
                    return False

        if ((self.features is None) and (other.features is not None)) or \
                ((self.features is not None) and (other.features is None)):
            return False

        if self.features is not None:
            for feature in self.features:
                if feature not in other.features:
                    return False

        if self.source != other.source:
            return False

        return True

    def __hash__(self):
        return hash((self.name, self.entities, self.features, self.source))

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
    def source(self) -> DataSourceBase:
        return self._source

    @property
    def tags(self) -> Dict[str, str]:
        return self._tags

    def get_entity(self, entity_name: str) -> Entity:
        for entity in self.entities:
            if entity.name == entity_name:
                return entity
        raise ValueError(f'Entity {entity_name} not found in feature view {self.name}')

    def get_feature(self, feature_name: str) -> Feature:
        for feature in self.features:
            if feature.name == feature_name:
                return feature
        raise ValueError(f'Feature {feature_name} not found in feature view {self.name}')

    def add_features_to_dataframe(self, dataframe: pd.DataFrame, feature_names: List[str],
                                  entity_name: Optional[str] = None) -> pd.DataFrame:
        if entity_name is None:
            raise NotImplementedError('Joins without entity not supported yet. ')

        entity = self.get_entity(entity_name)

        feature_values = self.source.select(
            columns=[entity] + feature_names,
            key=[entity],
            key_values=dataframe[entity.join_keys]
        )

        feature_values = cast_to_feature_types(
            dataframe=feature_values,
            features=[self.get_feature(feature_name) for feature_name in feature_names]
        )

        dataframe = dataframe.merge(feature_values, on=entity.join_keys, how='left')
        return dataframe
