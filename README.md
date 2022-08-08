# 🍟 Snax

The truly lightweight feature store. Heavily inspired by [feast](https://github.com/feast-dev/feast).

**The Problem Snax Solves**

Imagine you have a lot of different sources of your data, some data are possibly in `.csv` files, some in SQL database,
some in MongoDB or as documents in an S3 store.
Maybe some data are in local `.sqlite` file you use for developement,
but when that is completed, you want to switch to production OracleDB instead.

It would be nice to manage all these data sources in one place and have consistent and unified access to them via some
API.

**How it Solves That?**

`Snax` let's you define where your data sources are in a single directory, where you can put (in the form of a `.py`
files) their definitions and access those in your `python` code and then access them easily via a single API.

## Installation

Just run

```
pip install snax
```

## Getting Started

1. Create directory `feature_repo` and inside that create a file called `games.py` and paste the following inside it.

```python
import pandas as pd
from snax.data_sources.in_memory_data_source import InMemoryDataSource
from snax.entity import Entity
from snax.feature import Feature
from snax.feature_view import FeatureView
from snax.value_type import Int, String

raw_data = pd.DataFrame({
    'game_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'season': ['2020', '2020', '2020', '2020', '2020', '2020', '2020', '2020', '2020', '2020'],
    'home_goals': [5, 8, 3, 6, 4, 5, 2, 1, 0, 0],
    'away_goals': [3, 2, 1, 0, 2, 1, 0, 0, 0, 0],
    'start_timestamp': [
        '2022-17-02T15:50:00', '2022-12-02T16:30:00', '2022-12-02T16:30:00', '2022-12-02T16:30:00',
        '2022-17-02T15:50:00', '2022-12-02T16:30:00', '2022-12-02T16:30:00', '2022-12-02T16:30:00',
        '2022-17-02T15:50:00', '2022-12-02T16:30:00'
    ]
})

in_memory_games_data_source = InMemoryDataSource(name='games_in_memory', data=raw_data)
match_entity = Entity(name='game', join_keys=['game_id'])
nhl_games_csv_feature_view = FeatureView(
    name='games_in_memory',
    entities=[match_entity],
    features=[Feature('game_id', Int), Feature('season', String), Feature('home_goals', Int),
              Feature('away_goals', Int), Feature('start_timestamp', String)],
    source=in_memory_games_data_source)
```

2. Create python file called `main.py` and paste the following code inside it:

```python
import pandas as pd

from snax.feature_store import FeatureStore

FEATURE_STORE_PATH = '/path/to/my/feature_repo'
feature_store = FeatureStore(FEATURE_STORE_PATH)

entity_dataframe = pd.DataFrame({'game_id': [7, 6, 5, 4, 2, 10]})
entity_dataframe_with_values = feature_store.add_features_to_dataframe(
    dataframe=entity_dataframe,
    feature_names=[
        'games_in_memory:home_goals',
        'games_in_memory:away_goals',
        'games_in_memory:start_timestamp'
    ],
    entity_name='game',
)

print(entity_dataframe_with_values)
```

3. Change the `FEATURE_STORE_PATH` to where you created the `feature_repo` directory
4. Run the `main.py` file. You should get the following output:
    ```bash
    $ python main.py 
    game_id  home_goals  away_goals      start_timestamp
     7       2           0               2022-12-02T16:30:00
     6       5           1               2022-12-02T16:30:00
     5       4           2               2022-17-02T15:50:00
     4       6           0               2022-12-02T16:30:00
     2       8           2               2022-12-02T16:30:00
    10       0           0               2022-12-02T16:30:00
    ```

## Basic Concepts

### Feature

Name and a value type of some data. Represents some measurement we take about some object. E.g. the age of a customer in
years could be written as `Feature('age', Integer)`

### Entity

Name and a list of features whose values uniquely identify it. Represents some real-world object. E.g. a
customer could be represented as `Entity('customer', ['customer_id'])`.

### DataSource

An object responsible for retrieving tabular-like data about features and entities from some specific backend resource.
This could be in-memory pandas dataframe, csv file, sql database etc.
Given feature names it can return their values (for given entities) as pandas dataframe -- if these features exist
in the underlying resource. It is not concerned with data types.

### Feature View

Represents a table of features and entities that some of these features determine and also a link where and how to
retrieve their corresponding values from in the form of a `DataSource`.

### Feature Store

Collection of `FeatureView`s


