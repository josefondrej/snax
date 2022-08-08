from pathlib import Path

from snax.data_sources.csv_data_source import CsvDataSource
from snax.entity import Entity
from snax.feature import Feature
from snax.feature_view import FeatureView
from snax.value_type import Int, String, Timestamp, Float

data_path = Path(__file__).parent.parent.parent / 'data' / 'game.csv'

# Define DataSource Objects --------------------------------------------------------------------------------------------
nhl_games_csv_data_source = CsvDataSource(
    name='nhl_games_csv',
    csv_file_path=data_path
)

# Define Entity Objects ------------------------------------------------------------------------------------------------
match_entity = Entity(
    name='game',
    join_keys=['game_id']
)

# Define FeatureView Objects -------------------------------------------------------------------------------------------
nhl_games_csv_feature_view = FeatureView(
    name='nhl_games_csv',
    entities=[match_entity],
    features=[
        Feature('game_id', Int),
        Feature('season', String),
        Feature('type', String),
        Feature('date_time_GMT', Timestamp),
        Feature('away_team_id', Int),
        Feature('home_team_id', Int),
        Feature('away_goals', Int),
        Feature('home_goals', Int),
        Feature('outcome', String),
        Feature('home_rink_side_start', String),
        Feature('venue', String),
        Feature('venue_link', String),
        Feature('venue_time_zone_id', String),
        Feature('venue_time_zone_offset', Float),
        Feature('venue_time_zone_tz', String),
    ],
    source=nhl_games_csv_data_source
)
