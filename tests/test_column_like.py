from snax.column_like import get_colnames
from snax.entity import Entity
from snax.feature import Feature
from snax.value_type import Int


def test_get_colnames_entity():
    assert get_colnames(Entity('game', ['home_team_id', 'away_team_id'])) == ['home_team_id', 'away_team_id']


def test_get_colnames_feature():
    assert get_colnames(Feature('home_team_id', Int)) == ['home_team_id']


def test_get_colnames_str():
    assert get_colnames('home_team_id') == ['home_team_id']
