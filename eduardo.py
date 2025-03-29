from dataclasses import dataclass
import io
from typing import TypedDict
from dateutil.parser import parse as date_parse
import pandas as pd
import xml.etree.ElementTree as et
import datetime
import os
import logging
from enum import Enum
import requests
from sqlalchemy import create_engine, text, Integer, Float, Text
import boto3
import gc
import shutil


from stats import DataProcessingLogger, FlightStatistics
from excel_data import ExcelDataManager
from auxiliary import append_to_dict_lists, carrier_dict, city_dict, airport_dict, aircraft_dict
from models import Config, FlightType

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Create necessary directories for file operations only if running locally
if os.environ.get("AWS_EXECUTION_ENV") is None:
    logger.info("Running locally - creating directories for queries and results")
    os.makedirs('queries', exist_ok=True)     # Directory for XML query files
    os.makedirs('results', exist_ok=True)     # Directory for XML result files
    # Directory for processed CSV output
    os.makedirs('data/csv', exist_ok=True)
    # Directory for processed parquet output
    os.makedirs('data/parquet', exist_ok=True)

# Initialize the excel data manager and fetch the origins and destinations data
data_manager = ExcelDataManager()
origins, destinations, origin_and_destination = data_manager.get_origins_and_destinations()


class QueryType(Enum):
    ORIGIN = "origin"
    DESTINATION = "destination"


# Dictionaries to update based on the XML element
dictionaries = {
    'carrier': carrier_dict,
    'city': city_dict,
    'airport': airport_dict,
    'aircraft': aircraft_dict
}


@dataclass
class FlightDetails:
    """Data class to store flight details."""
    flight_type: FlightType
    s_id: int
    f_id: str
    carrier: str
    origin: str
    destination: str
    depart_dt: datetime
    arrive_dt: datetime
    aircraft: str
    width: str
    query_type: QueryType
    batch_num: int


class ReferenceDataDict(TypedDict):
    """Type definition for reference data dictionary."""
    aircraft: pd.DataFrame
    carrier: pd.DataFrame
    city: pd.DataFrame
    airport: pd.DataFrame
    countries: pd.DataFrame


def generate_xml_query_batch(query_type: QueryType, flight_type: FlightType, enable_batching: bool, batch_size: int):
    """
    Generates batched XML queries for flight data with maximum 15 origins/destinations per query.

    Args:
        query_type (QueryType): The type of query to generate.
        flight_type (FlightType): The type of flight to query for.
        batch_size (int): Maximum number of origins/destinations per query.
        enable_batching (bool): Flag to enable batching of queries.
        batch_size (int): Maximum number of origins/destinations per query.

    Returns:
        list: List of tuples containing (query_xml, batch_number)
    """
    # Helper function to create batches
    def batch_list(items, size):
        """Split a list into batches of specified size."""
        return [items[i:i + size] for i in range(0, len(items), size)]

    # Helper function to create base XML structure
    def create_base_xml():
        """Creates the common XML structure used in all queries."""
        search = et.Element(
            'search', data_manager.get_parameters_dict("search"))
        inputs = et.SubElement(search, 'inputs')
        search_control = et.SubElement(inputs, 'searchControl',
                                       data_manager.get_parameters_dict("searchControl"))

        # Add flight type
        permitted_service_type = et.SubElement(
            search_control, 'permittedServiceType')
        permitted_service_type.text = flight_type.name

        # Add slice element
        slice_element = et.SubElement(search_control, 'slice')

        # Add common elements
        date = et.SubElement(slice_element, "date")
        date.text = datetime.datetime.now().strftime('%Y-%m-%d')

        min_connection_time = et.SubElement(slice_element, "minConnectionTime")
        min_connection_time.text = data_manager.get_parameter_value(
            "minConnectionTime")

        max_stop_count = et.SubElement(slice_element, "maxStopCount")
        max_stop_count.text = data_manager.get_parameter_value("maxStopCount")

        days_after = et.SubElement(slice_element, "daysAfter")
        days_after.text = data_manager.get_parameter_value("daysAfter")

        summarizer = et.SubElement(search, 'summarizer')
        summarizer.text = data_manager.get_parameter_value("summarizer")

        return search, slice_element

    # Fetch IATA codes for origins, destinations, and origin-destination pairs
    IATA_CODE = 'IATA Code'
    origins_iata_codes: list[str] = origins[IATA_CODE].to_numpy().tolist()
    destinations_iata_codes: list[str] = destinations[IATA_CODE].to_numpy(
    ).tolist()
    origin_and_destination_iata_codes: list[str] = origin_and_destination[IATA_CODE].to_numpy(
    ).tolist()

    # Configure primary and secondary elements based on query type
    if query_type == QueryType.ORIGIN:
        # For ORIGIN queries: Batch origins, include all destinations
        primary_element = "origin"
        secondary_element = "destination"
        primary_codes = origin_and_destination_iata_codes + origins_iata_codes
        secondary_codes = destinations_iata_codes
    else:  # DESTINATION query
        # For DESTINATION queries: Batch destinations, include all origins
        primary_element = "destination"
        secondary_element = "origin"
        primary_codes = origin_and_destination_iata_codes + destinations_iata_codes
        secondary_codes = origins_iata_codes

    # List of query batches
    queries = []

    # NON-BATCHED MODE: Create a single query with all elements
    if not enable_batching:
        search, slice_element = create_base_xml()

        # Add ALL primary elements
        for code in primary_codes:
            element = et.SubElement(slice_element, primary_element)
            element.text = code

        # Add ALL secondary elements
        for code in secondary_codes:
            element = et.SubElement(slice_element, secondary_element)
            element.text = code

        # Return a single query with batch_num=1
        queries.append((search, 1))

        return queries

    # BATCHED MODE: Create multiple queries with batched primary elements
    # Create batches of the primary element
    primary_batches = batch_list(primary_codes, batch_size)

    # Generate queries for each batch
    for batch_num, batch_codes in enumerate(primary_batches, 1):
        search, slice_element = create_base_xml()

        # Add primary elements from this batch
        for code in batch_codes:
            element = et.SubElement(slice_element, primary_element)
            element.text = code

        # Add ALL secondary elements (not batched)
        for code in secondary_codes:
            element = et.SubElement(slice_element, secondary_element)
            element.text = code

        queries.append((search, batch_num))

    return queries


def save_query_to_file(query_type: QueryType, flight_type: FlightType, query_string: str, batch_num: int):
    """
    Saves the XML query to a file.

    Args:
        query (QueryType): The type of query generated.
        flight_type (FlightType): The type of flight to queried for.
        query_string (str): The XML query string to save.
        batch_num (int): The batch number to include in the filename.
    """
    query_filename = f'queries/query{query_type.name}-{flight_type.name}-batch{batch_num}.xml'
    logger.info(
        f'Running locally, storing query {query_type.name} for flight type {flight_type.name} batch {batch_num} to file {query_filename}')
    with open(query_filename, 'wb') as f:
        f.write(query_string)


def execute_query(offline_mode: bool, query_type: QueryType, flight_type: FlightType, query_string: str, batch_num: int) -> str:
    """
    Executes the XML query and returns the results.

    Args:
        offlineMode (bool): Flag to indicate if the function is running in offline
        query_type (QueryType): The type of query to execute.
        flight_type (FlightType): The type of flight to query for.
        query_string (str): The XML query string to execute.

    Returns:
        str: The results of the query
    """
    results_filename = f'results/results{query_type.name}-{flight_type.name}-batch{batch_num}.xml'

    if not offline_mode:
        logger.info(
            f'Executing query {query_type.name} for flight type {flight_type.name}')
        r = requests.post(
            'https://partner-client.google.com/travel/qpxconnect/production',
            headers={'Content-type': 'text/xml'},
            data=query_string
        )
        logger.info(
            f'Completed query {query_type.name} for flight type {flight_type.name}')
        results = r.text

        if os.environ.get("AWS_EXECUTION_ENV") is None:
            logger.info(
                f'Running locally, storing results of query {query_type.name} of type {flight_type.name} to file {results_filename}')
            with open(results_filename, 'w') as f:
                f.write(results)
    else:
        logger.info(
            f"Running offline, loading results from file {results_filename}")
        with open(results_filename, 'rb') as file:
            results = file.read()
            results = results.decode('utf-8')

    return results


def process_dict_updates(child: et.Element):
    """Update dictionaries based on the XML element."""
    tag = child.tag
    if tag in dictionaries:
        append_to_dict_lists(dictionaries[tag], child.attrib)


def update_flight_info(child: et.Element, state: FlightDetails):
    """Extract flight identification information."""
    state.f_id = f"{child.attrib['carrier']}{child.attrib['number']}"
    state.carrier = child.attrib['carrier']


def update_leg_info(child: et.Element, state: FlightDetails):
    """Extract leg information with error handling."""
    # Extract leg information
    try:
        state.origin = child.attrib['origin']
        state.destination = child.attrib['destination']
        state.depart_dt = child.attrib['departure']
        state.arrive_dt = child.attrib['arrival']
    except KeyError:
        state.origin = state.destination = ''
        state.depart_dt = state.arrive_dt = datetime.datetime.now()


def update_aircraft_info(child: et.Element, state: FlightDetails):
    """Extract aircraft information."""
    state.aircraft = child.attrib['name']
    state.width = child.attrib['width']


def update_flight_details(child: et.Element, state: FlightDetails):
    """Dispatch XML element to appropriate handler function."""

    # Dictionary of handler functions for different XML tags
    handlers = {
        'flight': lambda: update_flight_info(child, state),
        'leg': lambda: update_leg_info(child, state),
        'aircraft': lambda: update_aircraft_info(child, state)
    }

    # Call the handler function if the tag is found in the handlers dictionary
    if handler := handlers.get(child.tag):
        handler()


def create_flight_row(state: FlightDetails, row: int) -> pd.DataFrame:
    """Create a DataFrame row from current state."""
   # Columns for the dataframe
    cols = ['Flight Type', 'Origin', 'Destination', 'Flight #', 'Airline Code',
            'Aircraft', 'Width', 'Solution ID', 'Departure Time', 'Arrival Time', 'Query Type', 'Batch Number']

    # Values for the dataframe row
    values = [
        state.flight_type.name,
        state.origin,
        state.destination,
        state.f_id,
        state.carrier,
        state.aircraft,
        state.width,
        state.s_id,
        state.depart_dt,
        state.arrive_dt,
        state.query_type.name,
        state.batch_num,
    ]
    return pd.DataFrame(dict(zip(cols, values)), index=[row])


def validate_flight_details(flight_details: FlightDetails):
    """
    Validates that all required properties of a FlightDetails object are set.

    Args:
        flight_details (FlightDetails): The FlightDetails object to validate.

    Returns:
        bool: True if all properties are set, False otherwise.
    """
    required = {'flight_type', 'origin',
                'destination', 'f_id', 'carrier', 's_id'}
    return all(getattr(flight_details, field) for field in required)


def handle_create_df_row(child: et.Element, state: FlightDetails, row: int):
    """Increment the row counter and create a new row in the dataframe."""
    if child.tag == 'aircraft':
        if validate_flight_details(state):
            df = create_flight_row(state, row)
            return df
        else:
            raise ValueError("Flight details are not valid")


def parse_results(results: str, flight_type: FlightType, query_type: QueryType, batch_num: int):
    """Parse the XML results and extract relevant data."""

    # List to store dataframes for each query result
    dfs = []
    # A counter for solution IDs (Unique identifier for each flight solution)
    s_id = 0
    # A counter for rows in the dataframe
    row = 0
    # A flag to indicate if the itineraryFullDetail tag is encountered in the XML to start processing
    started_yet = False
    # Initialize the state object to None
    state = None

    # Create an ElementTree object from the XML results string
    results_tree = et.fromstring(results)

    for child in results_tree.iter('*'):
        # Update dictionaries based on the XML element
        process_dict_updates(child)

        # Mark the start of processing when the itineraryFullDetail tag is encountered
        if child.tag == 'itineraryFullDetail':
            started_yet = True

        # Skip processing if the itineraryFullDetail tag is not encountered
        if not started_yet:
            continue

        # Increment the solution ID and create a new FlightDetails object when the solution tag is encountered (New flight solution)
        if child.tag == 'solution':
            s_id += 1
            state = FlightDetails(
                flight_type,
                s_id,
                f_id=None,
                carrier=None,
                origin=None,
                destination=None,
                depart_dt=None,
                arrive_dt=None,
                aircraft=None,
                width=None,
                query_type=query_type,
                batch_num=batch_num,
            )

        # Skip processing until first solution
        if not state:
            continue

        # Update flight details based on the XML element
        update_flight_details(child, state)

        # Increment the row counter and create a new row in the dataframe
        try:
            df = handle_create_df_row(child, state, row)
            if df is not None:
                dfs.append(df)
                row += 1
        except ValueError as e:
            # Log an error message and skip the invalid row
            logger.error(f"Skipping invalid row: {e}")

    return dfs, row, s_id


def query_and_process_flights(config: Config, flight_types: list[FlightType] = [FlightType.F, FlightType.H, FlightType.L]):
    """
    Queries the API for flight data and processes the results.

    This function generates XML queries for flight data, sends them to an API, parses the results,
    and compiles statistics on the retrieved flight information.

    Args:
        config (Config): Configuration options for the query process
        flight_types (list): A list of FlightType enums to query for

    Returns:
        list: List of dataframes containing processed flight data
    """
    # Start timing the entire function
    total_start_time = datetime.datetime.now()
    logger.info(
        f"Starting flight data query and processing at {total_start_time.strftime('%H:%M:%S')}")

    # Create temp directory for intermediate storage - works in both environments
    tmp_dir = '/tmp/eduardo_processing' if os.environ.get(
        "AWS_EXECUTION_ENV") else 'tmp/eduardo_processing'
    os.makedirs(tmp_dir, exist_ok=True)
    logger.info(f"Created temp directory for processing: {tmp_dir}")

    # Track temporary storage
    temp_files = []

    # Batch configs
    batch_size = 15
    enable_batching = not config.b

    # Convert QueryType enum to list for easier iteration
    query_types = list(QueryType)

    # Track timing statistics
    query_times = []
    parsing_times = []

    # Process each flight type and query type
    for flight_type in flight_types:
        for query_type in query_types:
            flight_query_start = datetime.datetime.now()
            logger.info(f"Processing {flight_type.name}-{query_type.name} starting at "
                        f"{flight_query_start.strftime('%H:%M:%S')}")

            # Generate batched queries
            batch_gen_start = datetime.datetime.now()
            batched_queries = generate_xml_query_batch(
                query_type, flight_type, enable_batching, batch_size)
            batch_gen_duration = (
                datetime.datetime.now() - batch_gen_start).total_seconds()

            logger.info(
                f"Generated {len(batched_queries)} batch(es) for {query_type.name}-{flight_type.name} "
                f"in {batch_gen_duration:.2f} seconds")

            # Process each batch of queries
            for batch_idx, (query_xml, batch_num) in enumerate(batched_queries):
                batch_start_time = datetime.datetime.now()
                logger.info(f"  Processing batch {batch_num} ({batch_idx+1}/{len(batched_queries)}) "
                            f"starting at {batch_start_time.strftime('%H:%M:%S')}")

                # Convert XML to string
                query_string = et.tostring(
                    query_xml, xml_declaration=True, encoding='utf-8')

                # If running locally, save the query to a file with batch number
                if os.environ.get("AWS_EXECUTION_ENV") is None:
                    save_query_to_file(query_type, flight_type,
                                       query_string, batch_num)

                # Execute the query and get the results
                query_exec_start = datetime.datetime.now()
                results = execute_query(
                    config.o, query_type, flight_type, query_string, batch_num)
                query_exec_duration = (
                    datetime.datetime.now() - query_exec_start).total_seconds()
                query_times.append(query_exec_duration)

                # Memory optimization: Clear XML objects
                query_xml = None
                query_string = None

                # Parse the results for this batch
                parse_start_time = datetime.datetime.now()
                batch_dfs, row_count, sol_count = parse_results(
                    results, flight_type, query_type, batch_num)
                parse_duration = (datetime.datetime.now() -
                                  parse_start_time).total_seconds()
                parsing_times.append(parse_duration)

                # Memory optimization: Clear results string
                results = None

                # Save batch results to temporary pickle file (both local and Lambda)
                if batch_dfs:
                    batch_file = f"{tmp_dir}/batch_{flight_type.name}_{query_type.name}_{batch_num}.pkl"

                    if len(batch_dfs) > 0:
                        # Combine dataframes to reduce file operations
                        batch_df = pd.concat(batch_dfs, ignore_index=True)
                        batch_df.to_pickle(batch_file)

                        # Add file path to list for later processing
                        temp_files.append(batch_file)

                        # Memory optimization: Clear dataframes
                        batch_dfs = None
                        batch_df = None

                        # Force garbage collection
                        gc.collect()

                batch_duration = (datetime.datetime.now() -
                                  batch_start_time).total_seconds()
                logger.info(
                    f"  Batch {batch_num} completed in {batch_duration:.2f}s "
                    f"(Query: {query_exec_duration:.2f}s, Parse: {parse_duration:.2f}s): "
                    f"Found {row_count} rows with {sol_count} solutions")

            # Log flight-query completion time
            flight_query_duration = (
                datetime.datetime.now() - flight_query_start).total_seconds()
            logger.info(
                f"Completed {flight_type.name}-{query_type.name} in {flight_query_duration:.2f} seconds")

    # Process saved batch files
    logger.info(
        f"Processing {len(temp_files)} batch files into final dataset...")

    # List to store all dataframes
    all_dfs = []

    # Process files in smaller chunks to avoid memory spikes
    chunk_size = 2  # Process 2 files at a time

    for i in range(0, len(temp_files), chunk_size):
        chunk_files = temp_files[i:i+chunk_size]

        # Load each file in this chunk
        for file in chunk_files:
            try:
                df = pd.read_pickle(file)
                all_dfs.append(df)

                # Delete the file to free disk space
                os.remove(file)
            except Exception as e:
                logger.error(f"Error processing batch file {file}: {str(e)}")

        # Force garbage collection after each chunk
        gc.collect()
        logger.info(
            f"Processed batch files {i+1}-{min(i+chunk_size, len(temp_files))} of {len(temp_files)}")

    # Calculate and log processing summary
    total_duration = (datetime.datetime.now() -
                      total_start_time).total_seconds()
    avg_query_time = sum(query_times) / len(query_times) if query_times else 0
    avg_parse_time = sum(parsing_times) / \
        len(parsing_times) if parsing_times else 0
    logger.info(
        f"Query and processing complete in {total_duration:.2f} seconds")
    logger.info("Performance metrics:")
    logger.info(f"  - Total batches processed: {len(query_times)}")
    logger.info(f"  - Total rows retrieved: {sum(len(df) for df in all_dfs)}")
    logger.info(
        f"  - Average query execution time: {avg_query_time:.2f} seconds")
    logger.info(f"  - Average parsing time: {avg_parse_time:.2f} seconds")
    logger.info(
        f"  - Query execution: {sum(query_times):.2f}s ({sum(query_times)/total_duration*100:.1f}% of total time)")
    logger.info(
        f"  - Result parsing: {sum(parsing_times):.2f}s ({sum(parsing_times)/total_duration*100:.1f}% of total time)")

    # Clean up temp directory
    try:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    except Exception as e:
        logger.warning(f"Failed to clean up temp directory: {e}")

    # Print the flight data statistics
    flight_stats = FlightStatistics(
        all_dfs, flight_type_enum=FlightType, batch_size=batch_size)
    flight_stats.print_statistics_report()
    return all_dfs


def create_reference_dataframes() -> ReferenceDataDict:
    """
    Create dataframes from the reference dictionaries populated during parsing.

    Returns:
        ReferenceDataDict: Dictionary of reference dataframes with the following structure:
        - aircraft: DataFrame with aircraft information (name, type, width, etc.)
        - carrier: DataFrame with airline carrier information (code, name, country, etc.)
        - city: DataFrame with city information (code, name, country, etc.)
        - airport: DataFrame with airport information (code, name, city, country, latitude, longitude)
        - countries: DataFrame with country information (code, name, region, etc.)
    """

    logger.info("Creating reference dataframes from collected data")

    aircraft_df = pd.DataFrame(aircraft_dict)
    city_df = pd.DataFrame(city_dict)

    # Check for duplicate city codes and log them
    duplicate_cities = city_df['code'].duplicated(keep=False)
    if duplicate_cities.any():
        duplicate_city_codes = city_df.loc[duplicate_cities, 'code'].unique()
        logger.warning(
            f"Found {len(duplicate_city_codes)} duplicate city codes")
        logger.warning(f"Examples: {list(duplicate_city_codes)[:5]}")

        # Drop duplicates to ensure unique city codes for merge
        city_df = city_df.drop_duplicates(subset=['code'])
        logger.info(
            f"Kept first occurrence of each city code, now have {len(city_df)} unique cities")

    # Now perform the merge with the deduplicated city dataframe
    airport_df = pd.DataFrame(airport_dict)
    airport_df = airport_df.merge(
        city_df[['code', 'country']].rename(columns={'code': 'city'}),
        how='inner',
        on='city',
        validate='m:1'  # Many airports can belong to one city
    )
    airport_df = airport_df.drop_duplicates()

    carrier_df = pd.DataFrame(carrier_dict)
    carrier_df = carrier_df.drop_duplicates()

    # Get countries dataframe
    countries_df = data_manager.get_countries_dataframe()

    # Log reference data counts
    logger.info(f"Reference data created: {len(aircraft_df)} aircraft, {len(carrier_df)} carriers, "
                f"{len(city_df)} cities, {len(airport_df)} airports, {len(countries_df)} countries")

    return {
        'aircraft': aircraft_df,
        'carrier': carrier_df,
        'city': city_df,
        'airport': airport_df,
        'countries': countries_df
    }


def clean_raw_flight_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Combine raw flight dataframes and filter out invalid records.

    Args:
        df: Combined dataframe containing raw flight data

    Returns:
        pd.DataFrame: Clean combined flight data
    """
    logger.info(f"Starting data cleaning on {len(df)} rows")

    # Log initial state
    flight_types_count = df['Flight Type'].value_counts()
    logger.info("Flight type distribution (top 5):")
    for flight_type, count in flight_types_count.head().items():
        logger.info(f"  - {flight_type}: {count} rows")

    # Check for and log various data quality issues
    null_counts = df.isna().sum()
    problem_columns = null_counts[null_counts > 0]
    if not problem_columns.empty:
        logger.info("Found missing values in these columns:")
        for col, count in problem_columns.items():
            logger.info(f"  - {col}: {count} missing values")

    # Log and remove records with empty origin
    empty_origins = sum(df['Origin'] == '')
    logger.info(f"Filtering out {empty_origins} records with empty Origin")
    df = df[df['Origin'] != '']

    logger.info(f"Data cleaning completed, {len(df)} rows remain")
    return df


def enrich_flight_data(flight_data: pd.DataFrame, reference_data: dict) -> pd.DataFrame:
    """
    Enrich flight data with information from reference dataframes.

    Args:
        flight_data: DataFrame containing flight data
        reference_data: Dictionary of reference dataframes

    Returns:
        pd.DataFrame: Enriched flight data
    """
    logger.info(f"Starting data enrichment on {len(flight_data)} rows")

    # Define columns to use from airport data
    airport_column_keys = ['code', 'name', 'country', 'latitude', 'longitude']

    # Add origin airport information
    logger.info("Adding origin airport information...")
    before_count = len(flight_data)
    results = flight_data.merge(
        reference_data['airport'][airport_column_keys].rename(columns=dict(zip(airport_column_keys, [
            "Origin", "Origin Airport Name", "Origin Country", "Origin Latitude", "Origin Longitude"]))),
        how='inner', on='Origin')
    after_count = len(results)

    if after_count < before_count:
        logger.warning(
            f"Lost {before_count - after_count} rows when joining origin airport data")
        unknown_origins = set(
            flight_data['Origin']) - set(reference_data['airport']['code'])
        if unknown_origins:
            logger.warning(
                f"Unknown origins (sample): {list(unknown_origins)[:5]}")

    # Add destination airport information
    logger.info("Adding destination airport information...")
    before_count = len(results)
    results = results.merge(
        reference_data['airport'][airport_column_keys].rename(columns=dict(zip(airport_column_keys, [
            "Destination", "Destination Airport Name", "Destination Country", "Destination Latitude", "Destination Longitude"]))),
        how='inner', on='Destination')
    after_count = len(results)

    if after_count < before_count:
        logger.warning(
            f"Lost {before_count - after_count} rows when joining destination airport data")
        unknown_destinations = set(
            flight_data['Destination']) - set(reference_data['airport']['code'])
        if unknown_destinations:
            logger.warning(
                f"Unknown destinations (sample): {list(unknown_destinations)[:5]}")

    # Add carrier information
    logger.info("Adding carrier information...")
    before_count = len(results)
    results = results.merge(
        reference_data['carrier'][['code', 'name']].rename(columns={
            'code': 'Airline Code', 'name': 'Airline Name'}),
        how='inner', on='Airline Code')
    after_count = len(results)

    if after_count < before_count:
        logger.warning(
            f"Lost {before_count - after_count} rows when joining carrier data")

    # Parse datetime fields
    logger.info("Converting datetime fields...")
    results['Departure Time'] = results['Departure Time'].apply(
        lambda x: date_parse(x))
    results['Arrival Time'] = results['Arrival Time'].apply(
        lambda x: date_parse(x))

    # Sort by solution ID and departure time
    results.sort_values(['Solution ID', 'Departure Time'], inplace=True)

    logger.info(
        f"Data enrichment completed, {len(results)} rows with {len(results.columns)} columns")
    return results


def transform_flight_structure(flight_data: pd.DataFrame, reference_data: ReferenceDataDict) -> pd.DataFrame:
    """
    Transform flight data by grouping indirect flights into single itineraries.

    Args:
        flight_data: Enriched flight data
        reference_data: Dictionary of reference dataframes

    Returns:
        pd.DataFrame: Transformed flight data with grouped itineraries
    """
    logger.info(
        f"Starting flight structure transformation on {len(flight_data)} rows")

    # Group indirect flights by Solution ID
    cleaned = flight_data.groupby('Solution ID').agg({
        'Flight Type': 'first',
        'Flight #': [lambda x: list(x), 'count'],
        'Origin': 'first',
        'Origin Airport Name': 'first',
        'Origin Country': 'first',
        'Origin Latitude': 'first',
        'Origin Longitude': 'first',
        'Destination': ['first', 'last'],
        'Destination Airport Name': ['first', 'last'],
        'Destination Country': ['first', 'last'],
        'Destination Latitude': ['first', 'last'],
        'Destination Longitude': ['first', 'last'],
        'Airline Code': lambda x: '|'.join(list(x)),
        'Airline Name': lambda x: '|'.join(list(x)),
        'Aircraft': lambda x: '|'.join(list(x)),
        'Width': lambda x: '|'.join(list(x)),
        'Departure Time': 'first',
        'Arrival Time': 'last'
    })

    # Rename columns for clarity
    cleaned.columns = [
        'Flight Type', 'Flights', 'Num Stops', 'Origin IATA',
        'Origin Airport Name', 'Origin ISO Country Code', 'Origin Latitude',
        'Origin Longitude', 'Via IATA', 'Destination IATA', 'Via Airport Name',
        'Destination Airport Name', 'Via ISO Country Code', 'Destination ISO Country Code',
        'Via Latitude', 'Destination Latitude', 'Via Longitude', 'Destination Longitude',
        'Airline Code(s)', 'Airline Name(s)', 'Aircraft(s)', 'Width(s)',
        'Departure DT', 'Arrival DT'
    ]

    # Add country information - for this we need a countries dataframe
    try:
        countries_df = reference_data['countries']
        logger.info(
            f"Using countries reference data with {len(countries_df)} countries")

        cleaned = cleaned.merge(
            countries_df.rename(columns={'name': 'Origin Country', 'code': 'Origin ISO Country Code'})[
                ['Origin Country', 'Origin ISO Country Code']],
            how='inner', on='Origin ISO Country Code')

        cleaned = cleaned.merge(
            countries_df.rename(columns={'name': 'Destination Country', 'code': 'Destination ISO Country Code'})[
                ['Destination Country', 'Destination ISO Country Code']],
            how='inner', on='Destination ISO Country Code')
    except Exception as e:
        logger.warning(f"Could not add country information: {str(e)}")

    # Fix Via IATA when it's the same as destination
    cleaned['Via IATA'] = [cleaned['Via IATA'].iloc[i] if cleaned['Via IATA'].iloc[i]
                           != cleaned['Destination IATA'].iloc[i] else None for i in range(len(cleaned))]

    # Join flight numbers into string format
    cleaned['Flights'] = cleaned['Flights'].apply(lambda x: '|'.join(list(x)))

    logger.info(
        f"Transformed flight structure: {len(cleaned)} itineraries from {len(flight_data)} segments")

    return cleaned


def calculate_flight_metrics(flight_data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate additional flight metrics like duration.

    Args:
        flight_data: Transformed flight data

    Returns:
        pd.DataFrame: Flight data with additional metrics
    """
    logger.info(
        f"Calculating flight metrics for {len(flight_data)} itineraries")

    # Calculate flight duration
    flight_data['Duration DT'] = (
        flight_data['Arrival DT'] - flight_data['Departure DT']).apply(lambda x: x.total_seconds())
    flight_data['duration'] = flight_data['Duration DT'].apply(
        lambda x: round(x / (60**2), 2))

    # Add required columns if they don't exist
    required_columns = [
        'Origin IATA', 'Origin ISO Country Code', 'Origin Country',
        'Destination IATA', 'Destination ISO Country Code', 'Destination Country',
        'Departure Time (local)', 'Arrival Time (local)', 'Flight Num', 'Airline IATA',
        'duration', 'Arrival Time Hour', 'Origin Latitude', 'Origin Longitude',
        'Origin Airport Name', 'Origin Closed', 'Destination Latitude',
        'Destination Longitude', 'Destination Airport Name', 'Destination Closed',
        'Country Alert Updated', 'Alert Date', 'Alert Time', 'Message', 'Mode',
        'Num Stops', 'Current Capacity Status', 'Current Capacity Status Ind',
        'Passenger Flight Capacity % Change', 'Passenger Flight Capacity % Change Num',
        'Freighter Capacity % Change', 'Freighter Capacity % Change Num',
        'Flights', 'Via IATA', 'Departure DT', 'Arrival DT', 'Dummy Column 4',
        'Dummy Column 5', 'Dummy Column 6'
    ]

    for col in required_columns:
        if col not in flight_data.columns:
            flight_data[col] = [None] * len(flight_data)

    # Add timestamp for when data was updated
    flight_data['Updated'] = datetime.datetime.now().strftime(
        '%Y-%m-%d %H:%M:%S')

    logger.info(
        f"Flight metrics calculation completed with {len(flight_data.columns)} total columns")

    return flight_data


def save_to_database(flight_data: pd.DataFrame) -> None:
    """
    Save processed flight data to the database.

    Args:
        flight_data: Processed flight data to save
    """
    logger.info(f"Starting database save for {len(flight_data)} rows")

    # Define column types for SQL
    column_types = {
        'Flight Type': Text(),
        'Flights': Text(),
        'Num Stops': Integer(),
        'Origin IATA': Text(),
        'Origin Airport Name': Text(),
        'Origin ISO Country Code': Text(),
        'Origin Latitude': Float(),
        'Origin Longitude': Float(),
        'Via IATA': Text(),
        'Destination IATA': Text(),
        'Via Airport Name': Text(),
        'Destination Airport Name': Text(),
        'Via ISO Country Code': Text(),
        'Destination ISO Country Code': Text(),
        'Via Latitude': Float(),
        'Via Longitude': Float(),
        'Destination Latitude': Float(),
        'Destination Longitude': Float(),
        'Airline Code(s)': Text(),
        'Airline Name(s)': Text(),
        'Aircraft(s)': Text(),
        'Width(s)': Text(),
        'Departure DT': Text(),
        'Arrival DT': Text(),
        'Origin Country': Text(),
        'Destination Country': Text(),
        'Duration DT': Text(),
        'duration': Float(),
        # Additional columns
        'Departure Time (local)': Text(),
        'Arrival Time (local)': Text(),
        'Flight Num': Text(),
        'Airline IATA': Text(),
        'Arrival Time Hour': Text(),
        'Origin Closed': Text(),
        'Destination Closed': Text(),
        'Country Alert Updated': Text(),
        'Alert Date': Text(),
        'Alert Time': Text(),
        'Message': Text(),
        'Mode': Text(),
        'Current Capacity Status': Text(),
        'Current Capacity Status Ind': Text(),
        'Passenger Flight Capacity % Change': Text(),
        'Passenger Flight Capacity % Change Num': Text(),
        'Freighter Capacity % Change': Text(),
        'Freighter Capacity % Change Num': Text(),
        'Dummy Column 4': Text(),
        'Dummy Column 5': Text(),
        'Dummy Column 6': Text(),
        'Updated': Text()
    }

    try:
        # Get database connection parameters from environment
        DB_PROTOCOL = os.environ['DB_PROTOCOL']
        DB_HOST = os.environ['DB_HOST']
        DB_USER = os.environ['DB_USER']
        DB_PASS = os.environ['DB_PASS']
        DB_DATABASE = os.environ['DB_DATABASE']
        DB_TABLE = os.environ['DB_TABLE']

        connection_string = f"{DB_PROTOCOL}://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_DATABASE}"
        logger.info(f"Connecting to database at {DB_HOST}")
        engine = create_engine(connection_string)

        # Clean existing data
        connection = engine.connect()
        truncate_query = text("TRUNCATE TABLE EDUARDO_DATA")
        connection.execution_options(autocommit=True).execute(truncate_query)
        logger.info("Previous data truncated from database")

        # Helper function to chunk data
        def chunker(seq, size):
            """Split sequence into chunks of specified size."""
            return (seq[pos:pos + size] for pos in range(0, len(seq), size))

        # Upload in chunks
        chunksize = 100
        for i, cdf in enumerate(chunker(flight_data, chunksize)):
            cdf.to_sql(DB_TABLE, engine, if_exists='append',
                       index=False, method='multi', dtype=column_types)
            if i*chunksize % 1000 == 0:
                logger.info(f"{i*chunksize}/{len(flight_data)} rows uploaded")

        # Log row count in database
        number_of_rows = engine.execute(
            f"SELECT COUNT(*) FROM {DB_TABLE}").scalar()
        logger.info(
            f"Database save completed - {number_of_rows} rows in {DB_TABLE} table")

        engine.dispose()
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise


def process_flight_data(raw_flight_data: list[pd.DataFrame], config: Config) -> pd.DataFrame:
    """
    Main orchestrator function that processes raw flight data through a series of 
    transformations and enrichments.

    Args:
        raw_flight_data: List of dataframes containing raw flight data
        config: Configuration options

    Returns:
        pd.DataFrame: Processed and enriched flight data
    """

    # Initialize the processing logger
    processing_logger = DataProcessingLogger()

    # Combine raw dataframes for initial state
    initial_df = pd.concat(
        raw_flight_data, ignore_index=True) if raw_flight_data else pd.DataFrame()

    # Step 1: Create reference dataframes
    processing_logger.log_stage_start("Create Reference Dataframes")
    reference_data = create_reference_dataframes()
    #  summary dataframe with information about the reference dataframes
    ref_summary = pd.DataFrame({
        'dataframe': list(reference_data.keys()),
        'rows': [len(df) for df in reference_data.values()],
        'columns': [df.shape[1] for df in reference_data.values()]
    })
    processing_logger.log_stage_end("Create Reference Dataframes", ref_summary)

    # Step 2: Combine and clean the raw flight data
    combined_data = clean_raw_flight_data(initial_df)
    processing_logger.log_raw_data_cleaning(initial_df, combined_data)

    # Step 3: Enrich with reference data
    enriched_data = enrich_flight_data(combined_data, reference_data)
    processing_logger.log_data_enrichment(
        combined_data, enriched_data, reference_data)

    # Step 4: Transform data structure (group indirect flights)
    transformed_data = transform_flight_structure(
        enriched_data, reference_data)
    processing_logger.log_structure_transformation(
        enriched_data, transformed_data)

    # Step 5: Add calculated metrics like duration
    final_data = calculate_flight_metrics(transformed_data)
    processing_logger.log_metrics_calculation(transformed_data, final_data)

    # Step 6: Save to database if not disabled
    if not config.d:
        try:
            save_to_database(final_data)
            processing_logger.log_database_save(len(final_data), True)
        except Exception as e:
            processing_logger.log_database_save(len(final_data), False, str(e))

    # Print overall processing summary
    processing_logger.log_processing_summary()

    return final_data


def clean_up_temporary_files(flight_types: list[FlightType], config: Config) -> None:
    """
    Clean up temporary query and result files.

    Args:
        flight_types: List of flight types that were processed
    """

    if config.c:
        logger.info("Skipping cleanup due to config flag.")
        return

    logger.info("Cleaning up temporary files...")

    files_removed = 0

    # Remove files by pattern matching
    for flight_type in flight_types:
        for query_type in QueryType:
            # Find all matching query and result files with any batch number
            import glob

            # Match query files
            query_pattern = f"queries/query{query_type.name}-{flight_type.name}-batch*.xml"
            query_files = glob.glob(query_pattern)

            # Match result files
            result_pattern = f"results/results{query_type.name}-{flight_type.name}-batch*.xml"
            result_files = glob.glob(result_pattern)

            # Remove all matching files
            for file_path in query_files + result_files:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    files_removed += 1
                    logger.debug(f"Removed file: {file_path}")

    logger.info(f"Cleanup complete. Removed {files_removed} temporary files.")


def save_processed_data(data: pd.DataFrame, config: Config) -> None:
    """
    Save processed data to AWS S3 or local filesystem based on the environment.

    Args:
        data: DataFrame containing processed flight data
        config: Configuration options
    """
    # Skip saving if the 's' (skip save) flag is set
    if config.s:
        logger.info("Skipping data save due to config flag.")
        return

    today = datetime.datetime.now().strftime('%Y-%m-%d')
    running_on_aws = os.environ.get("AWS_EXECUTION_ENV") is not None

    if running_on_aws:
        logger.info("Running on AWS - saving data to S3...")
        try:
            # Get bucket name from environment variable
            bucket_name = os.environ.get('S3_BUCKET', 'logcluster-data-bucket')

            # Initialize S3 client with boto3
            # - Boto3 will detect and use the credentials available in the execution environment
            s3_client = boto3.client('s3')

            # Save CSV data
            csv_buffer = io.StringIO()
            data.to_csv(csv_buffer)
            csv_key = f"eduardo/datasets/csv/{today}.csv"

            s3_client.put_object(
                Bucket=bucket_name,
                Key=csv_key,
                Body=csv_buffer.getvalue()
            )
            logger.info(f"Saved CSV data to s3://{bucket_name}/{csv_key}")

            # Save Parquet data
            parquet_buffer = io.BytesIO()
            data.astype(str).to_parquet(parquet_buffer, compression='snappy')
            parquet_buffer.seek(0)  # Reset buffer position to beginning
            parquet_key = "eduardo/datasets/parquet/latest.parquet"

            s3_client.put_object(
                Bucket=bucket_name,
                Key=parquet_key,
                Body=parquet_buffer.getvalue()
            )
            logger.info(
                f"Saved Parquet data to s3://{bucket_name}/{parquet_key}")

        except Exception as e:
            logger.error(f"Error saving data to S3: {str(e)}")
    else:
        logger.info("Running locally - saving data to local files...")
        try:
            # Ensure directories exist
            os.makedirs('data/csv', exist_ok=True)
            os.makedirs('data/parquet', exist_ok=True)

            # Save CSV locally with today's date
            csv_path = f"data/csv/{today}.csv"
            data.to_csv(csv_path)
            logger.info(f"Saved CSV data to {csv_path}")

            # Save latest parquet file locally
            parquet_path = "data/parquet/latest.parquet"
            data.astype(str).to_parquet(parquet_path, compression='snappy')
            logger.info(f"Saved Parquet data to {parquet_path}")

        except Exception as e:
            logger.error(f"Error saving data locally: {str(e)}")


def run_eduardo(config: Config):
    """
    Main function that orchestrates the entire EDUARDO pipeline.

    Args:
        config: Configuration options
        flight_types: List of flight types to query

    Returns:
        pd.DataFrame: Final processed flight data
    """
    LARGE_DIVIDER = "==================================================================="

    logger.info(LARGE_DIVIDER)
    logger.info("                STARTING EDUARDO PIPELINE                ")
    logger.info(LARGE_DIVIDER)

    # Get selected flight types from Excel
    flight_types = data_manager.get_flight_types()
    if not flight_types:
        # Fallback to default types if none selected
        flight_types = [FlightType.F, FlightType.H, FlightType.J, FlightType.L]
        logger.info(
            f"No flight types selected in Excel, using defaults: {[ft.name for ft in flight_types]}")
    else:
        logger.info(
            f"Using flight types selected in Excel: {[ft.name for ft in flight_types]}")

    # Step 1: Query and retrieve flight data
    logger.info(LARGE_DIVIDER)
    logger.info("           PHASE 1: QUERYING FLIGHT DATA            ")
    logger.info(LARGE_DIVIDER)
    raw_flight_data = query_and_process_flights(config, flight_types)
    logger.info(LARGE_DIVIDER)
    logger.info("           END OF PHASE 1: QUERYING FLIGHT DATA     ")
    logger.info(LARGE_DIVIDER)

    # Step 2: Process and enrich the data if we have results
    logger.info(LARGE_DIVIDER)
    logger.info("       PHASE 2: PROCESSING AND ENRICHING FLIGHT DATA")
    logger.info(LARGE_DIVIDER)
    if raw_flight_data:
        processed_data = process_flight_data(raw_flight_data, config)
    else:
        logger.warning(
            "No flight data retrieved. Skipping data processing phase.")
        processed_data = pd.DataFrame()
    logger.info(LARGE_DIVIDER)
    logger.info(" END OF PHASE 2: PROCESSING AND ENRICHING FLIGHT DATA")
    logger.info(LARGE_DIVIDER)

    # Step 3: Save processed data to S3 or local filesystem
    logger.info(LARGE_DIVIDER)
    logger.info("           PHASE 3: SAVING PROCESSED DATA            ")
    logger.info(LARGE_DIVIDER)
    save_processed_data(processed_data, config)
    logger.info(LARGE_DIVIDER)
    logger.info("           END OF PHASE 3: SAVING PROCESSED DATA      ")
    logger.info(LARGE_DIVIDER)

    # Step 4: Clean up temporary files if not disabled
    logger.info(LARGE_DIVIDER)
    logger.info("           PHASE 4: CLEANING UP TEMPORARY FILES       ")
    logger.info(LARGE_DIVIDER)
    clean_up_temporary_files(flight_types, config)
    logger.info(LARGE_DIVIDER)
    logger.info("           END OF PHASE 4: CLEANING UP TEMPORARY FILES ")
    logger.info(LARGE_DIVIDER)

    # Final log message
    logger.info(LARGE_DIVIDER)
    logger.info("             EDUARDO PIPELINE COMPLETED                ")
    logger.info(LARGE_DIVIDER)
