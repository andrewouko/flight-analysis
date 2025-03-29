import logging
from eduardo import run_eduardo
from models import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Starting Eduardo Lambda Function")
    logger.info(f"Event: {event}")
    logger.info(f"Context: {context}")
    
    # Create the Config object from the command line arguments
    # - Set offline mode to False to make API calls and not read from local files
    # - Set database skip to True to skip updating the database with processed results
    # - Set cleanup skip to True as we are not actually creating any files when running on AWS Lambda
    # - Set batching disable to False to enable the batching of queries for optimization
    # - Set save skip to False to save results to CSV and Parquet files
    config = Config(o=False, d=True, c=True, b=False, s=False)
    
    # Run the eduardo pipeline
    run_eduardo(config)
