import importlib
import os
from pathlib import Path
from typing import List

from snax.data_source import DataSource
from snax.entity import Entity
from snax.feature_view import FeatureView
from snax.value_type import ValueType

DUMMY_ENTITY_ID = '__dummy_id'
DUMMY_ENTITY_NAME = '__dummy'
DUMMY_ENTITY = Entity(DUMMY_ENTITY_NAME, [DUMMY_ENTITY_ID], ValueType.STRING)


class RepoContents:
    def __init__(self, data_sources: List[DataSource] = None, feature_views: List[FeatureView] = None,
                 entities: List[Entity] = None):
        self._data_sources = data_sources or []
        self._feature_views = feature_views or []
        self._entities = entities or []

    @property
    def data_sources(self) -> List[DataSource]:
        return self._data_sources

    @property
    def feature_views(self) -> List[FeatureView]:
        return self._feature_views

    @property
    def entities(self) -> List[Entity]:
        return self._entities

    def get_feature_view(self, name: str) -> FeatureView:
        for feature_view in self.feature_views:
            if feature_view.name == name:
                return feature_view

    def get_entity(self, name: str) -> Entity:
        for entity in self.entities:
            if entity.name == name:
                return entity


def py_path_to_module(path: Path) -> str:
    return (
        str(path.relative_to(os.getcwd()))[:-len(".py")]
            .replace("./", "")
            .replace("/", ".")
    )


def parse_repo(repo_path: str) -> RepoContents:
    repo_path = Path(repo_path)
    repo_files = {path.resolve() for path in repo_path.glob('**/*.py')
                  if path.is_file() and path.name != '__init__.py'}

    repo_contents = RepoContents()
    data_sources_set = set()
    for repo_file in repo_files:
        module_path = py_path_to_module(repo_file)
        module = importlib.import_module(module_path)

        for attr_name in dir(module):
            obj = getattr(module, attr_name)
            if isinstance(obj, DataSource) and not any((obj is ds) for ds in repo_contents.data_sources):
                repo_contents.data_sources.append(obj)
                data_sources_set.add(obj)
            if isinstance(obj, FeatureView) and not any((obj is fv) for fv in repo_contents.feature_views):
                repo_contents.feature_views.append(obj)
            elif isinstance(obj, Entity) and not any((obj is entity) for entity in repo_contents.entities):
                repo_contents.entities.append(obj)

    repo_contents.entities.append(DUMMY_ENTITY)
    return repo_contents
