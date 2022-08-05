from typing import Union, List

from snax.entity import Entity
from snax.feature import Feature

ColumnLike = Union[Entity, Feature, str]


def get_feature_names(column_like: ColumnLike) -> List[str]:
    if isinstance(column_like, Entity):
        return column_like.join_keys
    elif isinstance(column_like, Feature):
        return [column_like.name]
    elif isinstance(column_like, str):
        return [column_like]
    else:
        raise TypeError(f'Unsupported type {type(column_like)}')


def get_features_names(column_likes: List[ColumnLike]) -> List[str]:
    column_likes_nested = [get_feature_names(column_like) for column_like in column_likes]
    return sum(column_likes_nested, [])
