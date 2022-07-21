import os
import shutil
import tempfile
from pathlib import Path


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
