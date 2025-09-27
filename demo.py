from healthcare_classification.logger import logging
from healthcare_classification.exception import HealthcareException
import sys

logging.info("welcome to our custome log")


try:
    a = 2/0
except Exception as e:
    raise HealthcareException(e, sys)