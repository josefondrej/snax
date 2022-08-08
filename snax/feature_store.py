from typing import List, Optional, Dict

import pandas as pd

from snax.data_sources.data_source_base import DataSourceBase
from snax.entity import Entity
from snax.feature_view import FeatureView
from snax.repo_contents import parse_repo, RepoContents


def group_features(features: List[str]) -> Dict[str, List[str]]:
    feature_dict = {}
    for feature in features:
        view_name, feature_name = feature.split(':')  # TODO: Define the splitter somewhere
        if view_name not in feature_dict:
            feature_dict[view_name] = []
        feature_dict[view_name].append(feature_name)

    return feature_dict


class FeatureStore:
    """
    Collection of various feature views
    """

    def __init__(self, repo_path: str):
        self._repo_path = repo_path
        self._repo_contents: RepoContents = parse_repo(repo_path)

    @property
    def repo_path(self) -> str:
        return self._repo_path

    def add_features_to_dataframe(self, dataframe: pd.DataFrame, feature_names: List[str],
                                  entity_name: Optional[str] = None) -> pd.DataFrame:
        """
        Retrieve features by their full name (feature_view_name:feature_name) and add them to the dataframe

        Args:
            dataframe: Entity's key values dataframe to add features to
            feature_names: List of full feature names to add to the dataframe in the format view_name:feature_name
            entity_name: Optional entity name to specify what columns to use for identifying the entity in the dataframe
                if it contains more columns than are required to identify the entity
        """
        feature_groups = group_features(feature_names)
        for view_name, feature_names in feature_groups.items():
            view = self.get_feature_view(view_name)
            dataframe = view.add_features_to_dataframe(dataframe, feature_names, entity_name)

        return dataframe

    def list_feature_views(self) -> List[FeatureView]:
        return self._repo_contents.feature_views

    def get_feature_view(self, name: str) -> FeatureView:
        return self._repo_contents.get_feature_view(name)

    def list_entities(self) -> List[Entity]:
        return self._repo_contents.entities

    def get_entity(self, name: str) -> Entity:
        return self._repo_contents.get_entity(name)

    def list_data_sources(self) -> List[DataSourceBase]:
        return self._repo_contents.data_sources

    def get_data_source(self, name: str) -> DataSourceBase:
        return self._repo_contents.get_data_source(name)
