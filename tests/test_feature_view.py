from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import Boolean, Integer

from snax.data_sources.examples.in_memory import create_users_with_nas
from snax.entity import Entity
from snax.feature import Feature
from snax.feature_view import FeatureView
from snax.value_type import String, Timestamp, Float


@pytest.fixture
def feature_view_users_with_nas():
    return FeatureView(
        name='users_with_nas',
        entities=[Entity('user', join_keys=['id']), Entity('full_user', join_keys=['id', 'string_id'])],
        features=[
            Feature(name='first_name', dtype=String),
            Feature(name='timestamp', dtype=Timestamp),
            Feature(name='is_subscribed', dtype=Boolean),
            Feature(name='age', dtype=Float),
            Feature(name='children', dtype=Integer)
        ],
        source=create_users_with_nas(),
    )


def test_add_feature_to_dataframe_on_single_key_entity(feature_view_users_with_nas: FeatureView):
    entity_dataframe = pd.DataFrame({
        'id': [1, 2, 3],
    })
    feature_values = feature_view_users_with_nas.add_features_to_dataframe(
        dataframe=entity_dataframe,
        feature_names=['first_name', 'timestamp', 'is_subscribed', 'age', 'children'],
        entity_name='user',
    )
    expected_feature_values = pd.DataFrame({
        'id': [1, 2, 3],
        'first_name': ['Cirillo', 'Codi', 'Marion'],
        'timestamp': [
            datetime(2021, 9, 4, 10, 41, 25),
            datetime(2021, 12, 24, 15, 17, 57),
            datetime(2021, 12, 9, 2, 56, 18)
        ],
        'is_subscribed': [True, True, False],
        'age': [45.8, 4.9, None],
        'children': [1.0, 3.0, 4.0]}
    )

    feature_values.reset_index(inplace=True, drop=True)
    expected_feature_values.reset_index(inplace=True, drop=True)

    assert feature_values.equals(expected_feature_values)


def test_add_feature_to_dataframe_on_multi_key_entity(feature_view_users_with_nas: FeatureView):
    entity_dataframe = pd.DataFrame({
        'id': [2, 3, 5],
        'string_id': ['b', 'c', 'ef'],
    })
    feature_values = feature_view_users_with_nas.add_features_to_dataframe(
        dataframe=entity_dataframe,
        feature_names=['first_name', 'timestamp', 'is_subscribed', 'age', 'children'],
        entity_name='full_user',
    )

    expected_feature_values = pd.DataFrame({
        'id': [2, 3, 5],
        'string_id': ['b', 'c', 'ef'],
        'first_name': ['Codi', 'Marion', None],
        'timestamp': [
            datetime(2021, 12, 24, 15, 17, 57),
            datetime(2021, 12, 9, 2, 56, 18),
            datetime(2022, 3, 30, 17, 29, 16)
        ],
        'is_subscribed': [True, False, False],
        'age': [4.9, None, 62.0],
        'children': [3.0, 4.0, 2.0]
    })

    feature_values.reset_index(inplace=True, drop=True)
    expected_feature_values.reset_index(inplace=True, drop=True)

    assert feature_values.equals(expected_feature_values)
