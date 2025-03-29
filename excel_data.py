from typing import Dict
import pandas as pd
import os
import boto3
from io import BytesIO
import logging

from models import FlightType

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

if os.environ.get("AWS_EXECUTION_ENV") is None:
    from dotenv import load_dotenv
    load_dotenv()

# Environment variables for excel file
S3_BUCKET = os.environ['S3_BUCKET']
S3_KEY = os.environ['S3_KEY']
FILE_NAME = os.environ["FILE_NAME"]

# Environment variables for sheet names
ORIGINS_AND_DESTINATIONS_SHEET = os.environ["ORIGINS_AND_DESTINATIONS_SHEET"]
COUNTRIES_SHEET = os.environ["COUNTRIES_SHEET"]
PARAMETERS_SHEET = os.environ["PARAMETERS_SHEET"]
FLIGHT_TYPES_SHEET = os.environ.get("FLIGHT_TYPES_SHEET")


class ExcelDataManager:
    def __init__(self):
        self.sheets: Dict[str, pd.DataFrame] = {}
        self._load_sheets()

    def _load_sheets(self):
        excel_file = self._read_excel_file()
        self.sheets[ORIGINS_AND_DESTINATIONS_SHEET] = excel_file.parse(
            ORIGINS_AND_DESTINATIONS_SHEET, dtype=str)
        self.sheets[COUNTRIES_SHEET] = excel_file.parse(
            COUNTRIES_SHEET, dtype=str)
        self.sheets[PARAMETERS_SHEET] = excel_file.parse(
            PARAMETERS_SHEET, dtype=str)
        self.sheets[FLIGHT_TYPES_SHEET] = excel_file.parse(
            FLIGHT_TYPES_SHEET, dtype=str)

    def _read_excel_file(self):
        """
        Reads the Excel file from either a local file or an S3 bucket depending on the environment.

        Returns:
            pd.ExcelFile: A Pandas ExcelFile object containing the Excel data.
        """
        if os.environ.get("AWS_EXECUTION_ENV") is None:
            # Local environment: Read from a local file
            return pd.ExcelFile(FILE_NAME, engine='openpyxl')
        else:
            # Lambda environment: Read from S3
            s3 = boto3.client('s3')
            try:
                # Download the file from S3
                obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY + FILE_NAME)
                return pd.ExcelFile(BytesIO(obj['Body'].read()), engine='openpyxl')
            except Exception as e:
                logger.error(f"Failed to read Excel file from S3: {e}")
                raise

    def _get_origins_and_destinations_dataframes(self):
        """Gets the dataframes for the origins and destinations sheet.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the origins and destinations sheet.
        """
        return self.sheets[ORIGINS_AND_DESTINATIONS_SHEET]

    def get_countries_dataframe(self):
        """Gets the dataframes for the countries sheet.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the countries sheet.
        """
        return self.sheets[COUNTRIES_SHEET]

    def _get_parameters_dataframe(self):
        """Gets the dataframes for the parameters sheet.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the parameters sheet.
        """
        return self.sheets[PARAMETERS_SHEET]

    def get_origins_and_destinations(self):
        """Gets the origins, destinations, and origin_and_destination dataframes.

        Returns:
            pd.DataFrame: A tuple of DataFrames containing the origins, destinations, and origin_and_destination dataframes.
        """
        origins_and_destinations_sheet_df = self._get_origins_and_destinations_dataframes()

        # Creates 3 dfs
        # - only appears as origin
        # - only appears as destination
        # - appears on both
        origins = origins_and_destinations_sheet_df[
            (origins_and_destinations_sheet_df['Origin'] == "X") &
            (origins_and_destinations_sheet_df['Destination'].isna())
        ]
        destinations = origins_and_destinations_sheet_df[
            (origins_and_destinations_sheet_df['Origin'].isna()) &
            (origins_and_destinations_sheet_df['Destination'] == "X")
        ]
        origin_and_destination = origins_and_destinations_sheet_df[
            (origins_and_destinations_sheet_df['Origin'] == "X") &
            (origins_and_destinations_sheet_df['Destination'] == "X")
        ]
        return origins, destinations, origin_and_destination

    def get_parameters_dict(self, element, columns=['Attribute', 'Value']):
        """Gets a dictionary of parameters for a given element.

        Args:
            element (str): The element to filter by.
            columns (list, optional): The column names to use for the dictionary. Defaults to ['Attribute', 'Value'].

        Returns:
            dict: A dictionary of parameters.
        """
        parameters_df = self._get_parameters_dataframe()
        temp = parameters_df[parameters_df['Element'] == element]
        return dict(zip(temp[columns[0]], temp[columns[1]]))

    def get_parameter_value(self, element):
        """Gets a parameter value for a given element.

        Args:
            element (str): The element to filter by.

        Returns:
            str: The parameter value.

        Raises:
            ValueError: If there is not exactly one value for the element.
        """
        parameters_df = self._get_parameters_dataframe()
        temp = parameters_df[(parameters_df['Element'] == element) &
                             (parameters_df['Attribute'].isna())]
        if len(temp) != 1:
            raise ValueError(
                f"Expected exactly one value for {element}, got {len(temp)}")
        return temp['Value'].iloc[0]

    def get_flight_types(self) -> list[FlightType]:
        """
        Get the selected flight types from the Flight Types sheet.

        Returns:
            list: List of FlightType enum values that are selected in the Excel sheet
        """
        logger.info("Getting selected flight types from Excel")

        # Get the flight types sheet
        flight_types_df = self.sheets[FLIGHT_TYPES_SHEET]

        # Filter for selected flight types
        selected_types = flight_types_df[flight_types_df['Selected'] == 'X']['Code'].tolist(
        )

        # Convert codes to FlightType enum values
        flight_types = []
        for code in selected_types:
            try:
                flight_type = FlightType[code]
                flight_types.append(flight_type)
                logger.debug(
                    f"Added flight type: {code} - {flight_type.value}")
            except KeyError:
                logger.warning(f"Unknown flight type code: {code}")

        logger.info(f"Found {len(flight_types)} selected flight types")
        return flight_types
