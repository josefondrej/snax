import pandas as pd
import pytest

import snax.data_sources.examples.in_memory
from snax.utils import frames_equal_up_to_row_ordering


@pytest.fixture
def nhl_data_source():
    return snax.data_sources.examples.in_memory.create_nhl_games()


def test_select_with_where(nhl_data_source):
    data = nhl_data_source.select(
        columns=['game_id', 'home_goals'],
        where_sql_query='game_id in (2016020045, 2017020812, 2015020314)'
    )
    expected_data = pd.DataFrame({'game_id': [2016020045, 2017020812, 2015020314], 'home_goals': [7, 3, 1]})
    assert frames_equal_up_to_row_ordering(data, expected_data)
