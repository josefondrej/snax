import importlib
from pathlib import Path
from typing import List

from snax.data_sources.data_source_base import DataSourceBase
from snax.entity import Entity
from snax.feature_view import FeatureView
from snax.value_type import ValueType

DUMMY_ENTITY_ID = '__dummy_id'
DUMMY_ENTITY_NAME = '__dummy'
DUMMY_ENTITY = Entity(DUMMY_ENTITY_NAME, [DUMMY_ENTITY_ID], ValueType.STRING)


class RepoContents:
    def __init__(self, data_sources: List[DataSourceBase] = None, feature_views: List[FeatureView] = None,
                 entities: List[Entity] = None):
        self._data_sources = data_sources or []
        self._feature_views = feature_views or []
        self._entities = entities or []

    @property
    def data_sources(self) -> List[DataSourceBase]:
        return self._data_sources

    @property
    def feature_views(self) -> List[FeatureView]:
        return self._feature_views

    @property
    def entities(self) -> List[Entity]:
        return self._entities

    def get_data_source(self, name: str) -> DataSourceBase:
        for data_source in self.data_sources:
            if data_source.name == name:
                return data_source

    def get_feature_view(self, name: str) -> FeatureView:
        for feature_view in self.feature_views:
            if feature_view.name == name:
                return feature_view

    def get_entity(self, name: str) -> Entity:
        for entity in self.entities:
            if entity.name == name:
                return entity


def import_as_module(repo_file_path: str, repo_path: str):
    module_relative_path = str(repo_file_path).replace(str(repo_path), '').replace('/', '.').replace('.py', '')
    top_level_dir_name = repo_path.name
    module_path = '_feature_store_repo.' + top_level_dir_name + module_relative_path
    spec = importlib.util.spec_from_file_location(module_path, repo_file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse_repo(repo_path: str) -> RepoContents:
    # TODO: Implement also for git repos
    repo_path = Path(repo_path)
    repo_file_paths = {path.resolve() for path in repo_path.glob('**/*.py')
                       if path.is_file() and path.name != '__init__.py'}

    repo_contents = RepoContents()
    data_sources_set = set()
    for repo_file_paths in repo_file_paths:
        module = import_as_module(repo_file_paths, repo_path)

        for attr_name in dir(module):
            obj = getattr(module, attr_name)
            if isinstance(obj, DataSourceBase) and not any((obj is ds) for ds in repo_contents.data_sources):
                repo_contents.data_sources.append(obj)
                data_sources_set.add(obj)
            if isinstance(obj, FeatureView) and not any((obj is fv) for fv in repo_contents.feature_views):
                repo_contents.feature_views.append(obj)
            elif isinstance(obj, Entity) and not any((obj is entity) for entity in repo_contents.entities):
                repo_contents.entities.append(obj)

    repo_contents.entities.append(DUMMY_ENTITY)
    return repo_contents
