from typing import Optional, Dict

from snax.data_source import DataSource


class CsvDataSource(DataSource):
    def __init__(self, name: str, csv_file_path: str, separator: str = ',',
                 field_mapping: Optional[Dict[str, str]] = None, tags: Optional[Dict] = None):
        super().__init__(name, field_mapping, tags)
        self._csv_file_path = csv_file_path
        self._separator = separator

    @property
    def csv_file_path(self) -> str:
        return self._csv_file_path

    @property
    def separator(self) -> str:
        return self._separator

    # TODO: Finish implementation