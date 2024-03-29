import os.path
from pathlib import Path

import pandas as pd

from snax._utils import copy_to_temp, frames_equal_up_to_row_ordering

dummy_directory_path = Path(__file__).parent / 'test_data' / 'dummy_directory'
dummy_file_path = dummy_directory_path / 'dummy_file.txt'
empty_dummy_directory_path = dummy_directory_path / 'empty_dummy_directory'


def test_copy_file():
    temp_path = copy_to_temp(dummy_file_path)
    assert os.path.exists(temp_path)


def test_copy_empty_dir():
    # Prepare empty directory in temporary location
    empty_directory_temp_path = copy_to_temp(empty_dummy_directory_path)
    temp_gitkeep_file_path = empty_directory_temp_path / '.gitkeep'
    if os.path.exists(temp_gitkeep_file_path):
        os.remove(temp_gitkeep_file_path)

    # Copy the empty directory to yet another temporary location
    empty_directory_temp_path_2 = copy_to_temp(empty_dummy_directory_path)
    assert os.path.exists(empty_directory_temp_path_2)


def test_copy_non_empty_dir():
    temp_path = copy_to_temp(dummy_directory_path)
    assert os.path.exists(temp_path)
    assert os.path.isdir(temp_path)
    assert sorted(os.listdir(temp_path)) == ['dummy_file.txt', 'empty_dummy_directory']


def test_frames_equal_up_to_row_ordering_frames_equal_shuffled():
    df1 = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    df2 = pd.DataFrame({'a': [2, 1, 3], 'b': [5, 4, 6]})
    assert frames_equal_up_to_row_ordering(df1, df2)


def test_frames_equal_up_to_row_ordering_frames_equal():
    df1 = pd.DataFrame({'a': [1, 2, 3], 'b': ["a", "b", "c"], "c": [True, False, True], "d": [1.0, 2.0, 3.0]})
    df2 = pd.DataFrame({'a': [1, 2, 3], 'b': ["a", "b", "c"], "c": [True, False, True], "d": [1.0, 2.0, 3.0]})
    assert frames_equal_up_to_row_ordering(df1, df2)


def test_frames_equal_up_to_row_ordering_frames_not_equal():
    df1 = pd.DataFrame({'a': [1, 2, 3], 'b': ["a", "b", "c"], "c": [True, False, True], "d": [1.0, 2.0, 3.0]})
    df2 = pd.DataFrame({'a': [1, 2, 3], 'b': ["a", "b", "c"], "c": [False, False, True], "d": [1.0, 2.0, 3.0]})
    assert not frames_equal_up_to_row_ordering(df1, df2)
