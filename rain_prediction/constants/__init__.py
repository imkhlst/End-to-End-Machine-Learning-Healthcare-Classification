import os
from datetime import date

DATABASE_NAME = "Rain-Prediction"

COLLECTION_NAME = "weatherdata"

MONGODB_URL_KEY = "MONGODB_URL"

PIPELINE_NAME: str = "rainpredition"

ARTIFACT_DIR: str = "artifact"

FILE_NAME: str = "weatherAUS.csv"
TRAIN_FILE_NAME: str = "train.csv"
TEST_FILE_NAME: str = "test.csv"

MODEL_FILE_NAME = "model.pkl"

"""
Data Ingestion related constant start with DATA_INGESTION VAR NAME
"""
DATA_INGESTION_COLLECTION_NAME: str = "weatherdata"
DATA_INGESTION_DIR_NAME: str = "data_ingestion"
DATA_INGESTION_FEATURE_STORE_DIR: str = "feature_store"
DATA_INGESTION_INGESTED_DIR: str = "ingested"
DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO: float = 0.2