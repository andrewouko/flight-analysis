import boto3
import json
import time
import os
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena = boto3.client('athena')

# Environment variables
DATABASE = os.environ.get('ATHENA_DATABASE', 'eduardo_flight_db_test')
TABLE = os.environ.get('ATHENA_TABLE', 'parquet')
OUTPUT_LOCATION = 's3://logcluster-eduardo-bucket/athena-results/'

# Maximum number of results to return per page
PAGE_SIZE = 100

def handler(event, context):
    logger.info("Received event: %s", json.dumps(event))
    try:
        query_params = event.get('queryStringParameters', {}) or {}
        logger.info("Query parameters: %s", query_params)

        page = int(query_params.get('page', 1))
        logger.info("Pagination - Page: %d", page)

        # Build the WHERE clause for filtering
        filters = []
        
        # Process specific fields with proper column names
        if 'origin' in query_params:
            filters.append(f"\"origin country\" = '{query_params['origin']}'")
        if 'destination' in query_params:
            filters.append(f"\"destination country\" = '{query_params['destination']}'")
        
        # Add other generic filters
        for key, value in query_params.items():
            if key not in ['page', 'origin', 'destination']:
                # Using double quotes for column names that may contain spaces
                filters.append(f"\"{key}\" = '{value}'")
                
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ''
        logger.info("Generated WHERE clause: %s", where_clause)

        # Create a query that supports pagination in Athena
        query = f"""
            SELECT 
                "flight type" AS flight_type,
                "origin iata" AS origin_iata,
                "origin country" AS origin_country,
                "destination iata" AS destination_iata, 
                "destination country" AS destination_country,
                "num stops" AS num_stops,
                TRY_CAST("duration" AS DOUBLE) AS duration_hours,
                "departure dt" AS departure_dt,
                "arrival dt" AS arrival_dt,
                "airline name(s)" AS airlines
            FROM {DATABASE}.{TABLE}
            {where_clause}
            LIMIT {PAGE_SIZE}
        """
        logger.info("Executing query: %s", query)

        # Start the Athena query
        query_execution = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': DATABASE},
            ResultConfiguration={'OutputLocation': OUTPUT_LOCATION}
        )
        query_execution_id = query_execution['QueryExecutionId']
        logger.info("Started Athena query with Execution ID: %s", query_execution_id)

        # Wait for the query to complete
        while True:
            query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
            state = query_status['QueryExecution']['Status']['State']
            logger.info("Query status: %s", state)
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1)

        if state != 'SUCCEEDED':
            error_message = "Query failed"
            if 'StateChangeReason' in query_status['QueryExecution']['Status']:
                error_message += f": {query_status['QueryExecution']['Status']['StateChangeReason']}"
            logger.error(error_message)
            raise Exception(error_message)

        # Fetch the query results
        results = athena.get_query_results(QueryExecutionId=query_execution_id)
        rows = results['ResultSet']['Rows']
        logger.info("Query returned %d rows", len(rows) - 1)  # Exclude header row

        # Parse the results
        headers = [cell['VarCharValue'] for cell in rows[0]['Data']]
        data = [
            {headers[i]: cell.get('VarCharValue', '') for i, cell in enumerate(row['Data'])}
            for row in rows[1:]
        ]
        logger.info("Parsed data: %s", json.dumps(data[:5]))  # Log first 5 rows for brevity

        return {
            'statusCode': 200,
            'body': json.dumps({
                'page': page,
                'pageSize': PAGE_SIZE,
                'data': data,
                'totalPages': 1  # For now, assume just one page
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'  # Enable CORS
            }
        }
    except Exception as e:
        logger.error("Error occurred: %s", str(e), exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'query_params': query_params
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'  # Enable CORS
            }
        }