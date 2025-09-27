import os
import sys

import numpy as np
import pandas as pd
import dill
import yaml

from healthcare_classification.exception import HealthcareException
from healthcare_classification.logger import logging


def read_yaml_file(file_path: str) -> dict:
    try:
        with open(file_path, "rb") as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise HealthcareException(e, sys) from e

def write_yaml_file(file_path: str, content: object, replace: bool = False) -> None:
    try:
        if replace:
            if os.path.exists(file_path):
                os.remove(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as file:
            yaml.dump(content, file)
    except Exception as e:
        raise HealthcareException(e, sys) from e

def load_object(file_path: str) -> object:
    logging.info("Entered the load_object method of utils.")
    
    try:
        with open(file_path, "rb") as file_obj:
            obj = dill.load(file_obj)
            
        logging.info("Exited the load_object method of utils.")
        return obj
    
    except Exception as e:
        raise HealthcareException(e, sys) from e

def save_numpy_array_data(file_path: str, array: np.array):
    """
    Saves numpy array data to file.

    Args:
        file_path (str): Location of file to be saved.
        array (np.array): Data array saved.
    """
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)
        with open(file_path, "wb") as file_obj:
            np.save(file_obj, array)
    except Exception as e:
        raise HealthcareException(e, sys) from e

def load_numpy_array_data(file_path: str) -> np.array:
    """
    Loads numpy array data from file.

    Args:
        file_path (str): Location of file to be loaded.

    Returns:
        np.array: Data array loaded.
    """
    try:
        with open(file_path, "rb") as file_obj:
            return np.load(file_obj)
    except Exception as e:
        raise HealthcareException(e, sys) from e

def save_obj(file_path: str, obj: object) -> None:
    logging.info("Entered the save_object method of utils.")
    
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file_obj:
            dill.dump(obj, file_obj)
    
    except Exception as e:
        raise HealthcareException(e, sys) from e

def drop_columns(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """
    Drops the columns from a pandas DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        cols (list): List of columns name to be dropped.

    Returns:
        pd.DataFrame: The Dataframe with dropped columns.
    """
    logging.info("Entered drop_columns method of utils.")
    
    try:
        df = df.drop(columns=cols, axis=1)
        
        logging.info("Exited the drop_columns method of utils.")
        return df
    except Exception as e:
        raise HealthcareException(e, sys) from e