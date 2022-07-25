# TODO

- [x] Make example datasets smaller
- [ ] Unify the query language in `DataSource` so it does not depend on the type of the `DataSource`
- [x] Implement `utils/copy_to_temp.py`
- [x] Use `utils/copy_to_temp.py` in `test_repo_contents.py`, `test_csv_data_source.py`, `test_feature_store.py`
- [x] Complete `test_csv_data_source.py` (& its related tasks)
- [ ] Complete `test_feature_store.py` (& its related tasks)
- [x] Add dataset that has missings in string, float, int, datetime columns
- [x] Add tests to `test_csv_data_source.py` on missing values
- [x] Implement in-memory data source (& perhaps derive csv data source & others from it)
- [ ] Remove csv data sources `.data` property
- [ ] Implement in-memory data source
- [ ] Refactor csv data source so it uses in-memory data source under the hood
- [ ] Refactor generally usable tests from csv data source to `test_data_source.py` [new] and run them agains all
  data sources as parametrized tests
- [ ] Add data source test with non-empty `field_mapping`