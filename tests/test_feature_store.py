from pathlib import Path
from unittest import skip

import pandas as pd
from pandas.testing import assert_frame_equal

from snax.example_feature_repos import sports_feature_repo
from snax.feature_store import FeatureStore


@skip('TODO: FeatureStore.add_features_to_dataframe not implemented yet')
def test_add_features_to_dataframe():
    sports_feature_repo_path = Path(sports_feature_repo.__file__).parent
    feature_store = FeatureStore(repo_path=sports_feature_repo_path)

    nhl_games_data_source = feature_store.get_data_source('nhl_games_csv')
    game_entity = feature_store.get_entity('game')
    where_sql_query = "(venue_time_zone_id=='America/Winnipeg' or venue_time_zone_id.str.contains('Vancouver')) and season==20172018"
    game_ids = nhl_games_data_source.select(
        columns=[game_entity],
        where_sql_query=where_sql_query
    )
    expected_game_ids = pd.DataFrame({'game_id': [2017020001, 2017020423]})

    assert game_ids.reset_index(drop=True).equals(expected_game_ids.reset_index(drop=True))

    feature_dataframe = feature_store.add_features_to_dataframe(
        dataframe=game_ids,
        features=[
            'nhl_games_csv:outcome',
            'nhl_games_csv:venue'
        ]
    )

    expected_feature_dataframe = pd.DataFrame({
        'game_id': {22: 2017020001, 40: 2017020423},
        'outcome': {22: 'away win REG', 40: 'home win REG'},
        'venue': {22: 'Bell MTS Place', 40: 'Rogers Arena'}
    })

    assert_frame_equal(feature_dataframe, expected_feature_dataframe)
