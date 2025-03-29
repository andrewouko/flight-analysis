import argparse
from dotenv import load_dotenv
from eduardo import Config, run_eduardo
import logging
import sys

# Load the environment variables from the .env file
load_dotenv()

# Set up the logging
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger()

# Parse the command line arguments
parser = argparse.ArgumentParser(description="Just an example",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-o", action="store_true", help="Don't download new data.")
parser.add_argument("-d", action="store_true",
                    help="Don't upload results to the database")
parser.add_argument("-c", action="store_true",
                    help="Don't cleanup query and result files")
parser.add_argument("-b", action="store_true",
                    help="Disable batching of queries to Google QPX Express API")
parser.add_argument("-s", action="store_true",
                    help="Don't save results to CSV and Parquet files")
args = parser.parse_args()

# Create the Config object from the command line arguments
config = Config(o=args.o, d=args.d, c=args.c, b=args.b)

# Run the main function
if (__name__ == '__main__'):
    run_eduardo(config)
