from snax.column_like import get_feature_names
from snax.entity import Entity
from snax.feature import Feature
from snax.value_type import Int


def test_get_colnames_entity():
    assert get_feature_names(Entity('game', ['home_team_id', 'away_team_id'])) == ['home_team_id', 'away_team_id']


def test_get_colnames_feature():
    assert get_feature_names(Feature('home_team_id', Int)) == ['home_team_id']


def test_get_colnames_str():
    assert get_feature_names('home_team_id') == ['home_team_id']
