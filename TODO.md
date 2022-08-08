# TODO

- [ ] Implement `FeatureStore.add_features_to_dataframe`
- [ ] Add option to keep full feature names (`view:feature`) in `add_features_to_dataframe`
- [ ] Handle reserved column names in oracle data source
- [ ] Unify the query language in `DataSource` so it does not depend on the type of the `DataSource`
- [ ] Implement optional entities (the `__dummy` entity is already preparation for that)
- [ ] Support not only entity-indexed but entity-in-time indexed data and point-in-time joins
- [ ] Support richer string timestamp formats when converting to `Timestamp`
- [ ] Support richer None/NaN formats in list ValueTypes casting from string

- [x] Add select by key values option to data source `select(..., key_values: pd.DataFrame = None, ...)`
- [x] Add few data source specific tests to test that at least some `where_sql_query` parameter values work
- [x] Test for inserting new data into empty data source
- [x] Test inserting with multi-keys
- [x] Make example datasets smaller
- [x] Implement `utils/copy_to_temp.py`
- [x] Use `utils/copy_to_temp.py` in `test_repo_contents.py`, `test_csv_data_source.py`, `test_feature_store.py`
- [x] Complete `test_csv_data_source.py` (& its related tasks)
- [x] Complete `test_feature_store.py`
- [x] Add dataset that has missings in string, float, int, datetime columns
- [x] Add tests to `test_csv_data_source.py` on missing values
- [x] Implement in-memory data source (& perhaps derive csv data source & others from it)
- [x] Remove csv data sources `.data` property
- [x] Refactor generally usable tests from csv data source to `test_data_source.py` [new] and run them agains all
  data sources as parametrized tests
- [x] Add data source test with non-empty `field_mapping` & test it
- [x] Add Oracle data source
