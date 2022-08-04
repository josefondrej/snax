import os
import shutil
import tempfile
from pathlib import Path

import pandas as pd


def copy_to_temp(path: str) -> str:
    """
    Copy a file or a directory to a temporary location and return the path to that location.
    Args:
        path: Path of the file / directory to be copied

    Returns:
        Path to the copied file / directory
    """
    snax_tmp_path = Path(tempfile.gettempdir()) / 'snax_tmp'
    os.makedirs(snax_tmp_path, exist_ok=True)

    target_path = snax_tmp_path / Path(path).name

    if os.path.isdir(path):
        if os.path.exists(target_path):
            shutil.rmtree(target_path)

        return shutil.copytree(path, target_path)
    else:
        if os.path.exists(target_path):
            os.remove(target_path)

        return shutil.copy(path, target_path)


def frames_equal_up_to_row_ordering(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    if (df1.columns != df2.columns).any():
        return False
    merged = df1.merge(df2, on=df1.columns.tolist(), how='outer', indicator=True)
    return (merged['_merge'] == 'both').all()
