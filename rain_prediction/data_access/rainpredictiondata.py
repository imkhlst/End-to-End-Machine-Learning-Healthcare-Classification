from rain_prediction.configuration.mongo_db_connection import MongoDBClient
from rain_prediction.constants import DATABASE_NAME
from rain_prediction.exception import RainPredictionException
import pandas as pd
import sys
from typing import Optional

class RainPredictionData:
    """
    This class help to export netire mongo db record as pandas DataFrame.
    """
    def __init__(self):
        try:
            self.mongo_client = MongoDBClient(database_name=DATABASE_NAME)
        except Exception as e:
            raise RainPredictionException(e, sys)
    
    def export_colletion_as_dataframe(self, collection_name: str, database_name: Optional[str]=None) -> pd.DataFrame:
        try:
            """
            Export entire collection as dataframe.
            Return:
                pd.DataFrame: DataFrame of colletion 
            """
            if database_name is None:
                collection = self.mongo_client.database[collection_name]
            else:
                collection = self.mongo_client[database_name][collection_name]
            
            df = pd.DataFrame(list(collection.find()))
            if "_id" in df.columns.to_list():
                df = df.drop(columns=["_id"], axis=1)
            return df
        except Exception as e:
            raise RainPredictionException(e, sys)