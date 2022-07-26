from typing import List

import pandas as pd

from snax.data_sources.data_source_base import DataSourceBase
from snax.entity import Entity
from snax.feature_view import FeatureView
from snax.repo_contents import parse_repo, RepoContents


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

    def add_features_to_dataframe(self, dataframe: pd.DataFrame, features: List[str]) -> pd.DataFrame:
        """
        Retrieve features by their full name (feature_view_name:feature_name) and add them to the dataframe

        Args:
            dataframe: Dataframe to add features to
            features: List of full feature names to add to the dataframe
        """
        raise NotImplemented('TODO: Implement')

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
