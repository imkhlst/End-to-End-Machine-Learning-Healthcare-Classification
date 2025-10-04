import os
import sys

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

from rain_prediction.entity.config_entity import DataIngestionConfig
from rain_prediction.entity.artifact_entity import DataIngestionArtifact
from rain_prediction.exception import RainPredictionException
from rain_prediction.logger import logging
from rain_prediction.data_access.rainpredictiondata import RainPredictionData


class DataIngestion:
    def __init__(self, data_ingestion_config:DataIngestionConfig=DataIngestionConfig()):
        """_summary_

        Args:
            data_ingestion_config (DataIngestionConfig, optional): _description_. Defaults to DataIngestionConfig().
        """
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise RainPredictionException(e, sys)
    
    def export_data_into_feature_store(self) -> pd.DataFrame:
        """_summary_

        Returns:
            pd.DataFrame: _description_
        """
        try:
            logging.info(f"Exporting data from MongoDB.")
            rainprediction_data = RainPredictionData()
            df = rainprediction_data.export_colletion_as_dataframe(
                collection_name=self.data_ingestion_config.collection_name
            )
            logging.info(f"Shape of DataFrame: {df.shape}.")
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)
            logging.info(f"Saving exported data into feature store file path: {feature_store_file_path}.")
            df.to_csv(feature_store_file_path, index=False, header=True)
            return df
        
        except Exception as e:
            raise RainPredictionException(e, sys)
    
    def split_data_as_train_test(self, dataframe: pd.DataFrame) -> None:
        """_summary_

        Args:
            dataframe (pd.DataFrame): _description_
        """
        logging.info("Entered split_data_as_train_test method of DataIngestion class.")
        
        try:
            logging.info("Performing train test split on the DataFrame.")
            train_set, test_set = train_test_split(dataframe, test_size=self.data_ingestion_config.train_test_split_ratio, random_state=42)
            logging.info("DataFrame has been split into train set and test set.")
            logging.info("Exited split_data_as_train_test method of DataIngestion class.")
            
            logging.info("Creating training and testing directory path.")
            train_dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            test_dir_path = os.path.dirname(self.data_ingestion_config.testing_file_path)
            os.makedirs(train_dir_path, exist_ok=True)
            os.makedirs(test_dir_path, exist_ok=True)
            logging.info("Training and Testing Directory has been created.")
            
            logging.info("Exporting train and test file path.")
            train_set.to_csv(self.data_ingestion_config.training_file_path, index=False, header=True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path, index=False, header=True)
            logging.info("Train and test data has been exported.")
            
        except Exception as e:
            raise RainPredictionException(e, sys) from e
    
    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        """_summary_

        Returns:
            DataIngestionArtifact: _description_
        """
        logging.info("Entered initiate_data_ingestion method of DataIngestion class.")
        
        try:
            df =self.export_data_into_feature_store()
            
            logging.info("Got the data form MongoDB.")
            
            self.split_data_as_train_test(df)
            
            logging.info("Train test split on the dataset completed.")
            
            logging.info("Exited initiate_data_ingestion method of DataIngestion class.")
            
            data_ingestion_artifact = DataIngestionArtifact(
                trained_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path
            )
            
            logging.info(f"Data Ingestion artifact: {data_ingestion_artifact}")
            return data_ingestion_artifact
        
        except Exception as e:
            raise RainPredictionException(e, sys) from e