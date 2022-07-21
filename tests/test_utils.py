import os.path
from pathlib import Path

from snax.utils import copy_to_temp

dummy_directory_path = Path(__file__).parent / 'test_data' / 'dummy_directory'
dummy_file_path = dummy_directory_path / 'dummy_file.txt'
empty_dummy_directory_path = dummy_directory_path / 'empty_dummy_directory'


def test_copy_file():
    temp_path = copy_to_temp(dummy_file_path)
    assert os.path.exists(temp_path)


def test_copy_empty_dir():
    temp_path = copy_to_temp(empty_dummy_directory_path)
    assert os.path.exists(temp_path)


def test_copy_non_empty_dir():
    temp_path = copy_to_temp(dummy_directory_path)
    assert os.path.exists(temp_path)
    assert os.path.isdir(temp_path)
    assert sorted(os.listdir(temp_path)) == ['dummy_file.txt', 'empty_dummy_directory']
