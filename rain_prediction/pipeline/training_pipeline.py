import sys
from rain_prediction.exception import RainPredictionException
from rain_prediction.logger import logging
from rain_prediction.entity.config_entity import DataIngestionConfig
from rain_prediction.entity.artifact_entity import DataIngestionArtifact
from rain_prediction.component.data_ingestion import DataIngestion


class TrainPipeline:
    def __init__(self):
        self.data_ingestion_config = DataIngestionConfig()
    
    def start_data_ingestion(self) -> DataIngestionArtifact:
        """_summary_

        Returns:
            DataIngestionArtifact: _description_
        """
        try:
            logging.info("Entered the start_data_ingestion method of TrainPipeline class.")
            logging.info("Getting the data from MongoDB.")
            data_ingestion = DataIngestion(data_ingestion_config=self.data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            logging.info("Got the train_set and test_set from MongoDB")
            logging.info("Exited the start_data_ingestion method of TrainPipeline class.")
            return data_ingestion_artifact
        except Exception as e:
            raise RainPredictionException(e, sys) from e
    
    def run_pipeline(self,) -> None:
        """_summary_
        """
        try:
            data_ingestion_artifact = self.start_data_ingestion()
        except Exception as e:
            raise RainPredictionException(e, sys) from e