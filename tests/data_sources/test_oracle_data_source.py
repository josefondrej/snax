import pandas as pd
import pytest

import snax.data_sources.examples.oracle
from snax.utils import frames_equal_up_to_row_ordering


@pytest.fixture
def nhl_data_source():
    data_source = snax.data_sources.examples.oracle.create_nhl_games()
    if data_source is None:
        pytest.skip('Oracle data source not available')
    return data_source


def test_select_with_where(nhl_data_source):
    data = nhl_data_source.select(
        columns=['game_id', 'home_goals'],
        where_sql_query='game_id in (2016020045, 2017020812, 2015020314)'
    )
    expected_data = pd.DataFrame({'game_id': [2016020045, 2017020812, 2015020314], 'home_goals': [7, 3, 1]})
    assert frames_equal_up_to_row_ordering(data, expected_data)
