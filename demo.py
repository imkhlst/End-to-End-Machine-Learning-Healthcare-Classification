from rain_prediction.logger import logging
from rain_prediction.exception import RainPredictionException
import sys

logging.info("welcome to our custome log")


try:
    a = 2/0
except Exception as e:
    raise RainPredictionException(e, sys)