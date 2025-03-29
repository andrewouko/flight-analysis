# Flight Query Lambda

This project contains a Lambda function and deployment script for querying an Athena database via an API Gateway.

## Prerequisites

1. **AWS CLI**: Ensure the AWS CLI is installed and configured with appropriate permissions.
   - Run `aws configure` to set up your credentials.
   - Verify credentials with `aws sts get-caller-identity`.
2. **Python**: Install Python 3.9 or later.
3. **IAM Role**: The deployment script will create an IAM role with the necessary permissions.
4. **Athena Database**: Ensure you have an Athena database and table set up.
5. **S3 Bucket**: Create an S3 bucket for Athena query results.

## Deployment

1. Navigate to the project directory:

   ```bash
   cd /flight_query_lambda
   ```

2. Run the deployment script:

   ```bash
   ./deploy.sh
   ```

   The script will:
   - Check for AWS credentials.
   - Load environment variables with defaults
   - Package the Lambda function.
   - Create or update the Lambda function.
   - Create or update the API Gateway.
   - Deploy the API Gateway.

3. After deployment, the script will output the API Gateway URL. Note this URL for making requests.

   - Example output:

     ```bash
     API Gateway URL: https://<API_ID>.execute-api.<region>.amazonaws.com/prod/
     ```

   - Use this URL to query the API with the parameters listed below.

## Usage

### Query Parameters

The following query parameters can be used to filter results:

- `flight_type`: string
- `flights`: string
- `num_stops`: string
- `origin_iata`: string
- `origin_airport_name`: string
- `origin_iso_country_code`: string
- `origin_latitude`: string
- `origin_longitude`: string
- `via_iata`: string
- `destination_iata`: string
- `via_airport_name`: string
- `destination_airport_name`: string
- `via_iso_country_code`: string
- `destination_iso_country_code`: string
- `via_latitude`: string
- `destination_latitude`: string
- `via_longitude`: string
- `destination_longitude`: string
- `airline_code(s)`: string
- `airline_name(s)`: string
- `aircraft(s)`: string
- `width(s)`: string
- `departure_dt`: string
- `arrival_dt`: string
- `origin_country`: string
- `destination_country`: string
- `duration_dt`: string
- `duration`: string
- `departure_time_(local)`: string
- `arrival_time_(local)`: string
- `flight_num`: string
- `airline_iata`: string
- `arrival_time_hour`: string
- `origin_closed`: string
- `destination_closed`: string
- `country_alert_updated`: string
- `alert_date`: string
- `alert_time`: string
- `message`: string
- `mode`: string
- `current_capacity_status`: string
- `current_capacity_status_ind`: string
- `passenger_flight_capacity_%_change`: string
- `passenger_flight_capacity_%_change_num`: string
- `freighter_capacity_%_change`: string
- `freighter_capacity_%_change_num`: string
- `dummy_column_4`: string
- `dummy_column_5`: string
- `dummy_column_6`: string
- `updated`: string

### Sample Query

To query the API, use the API Gateway URL provided after deployment. You can use `curl` or any HTTP client.

#### Example Request

```bash
curl "https://<API_ID>.execute-api.<region>.amazonaws.com/prod/?column1=value1&column2=value2&page=1"
```

- Replace `<API_ID>` and `<region>` with the values from the deployment output.
- Replace `column1`, `column2`, and their values with the desired query parameters.
- Use the `page` parameter for pagination.

#### Example Response

```json
{
    "page": 1,
    "pageSize": 100,
    "data": [
        {
            "flight_type": "J",
            "origin_iata": "FCO",
            "origin_country": "Italy",
            "destination_iata": "NBO",
            "destination_country": "Kenya",
            "num_stops": "35",
            "duration_hours": "415.92",
            "departure_dt": "2025-03-27 14:55:00+01:00",
            "arrival_dt": "2025-04-14 00:50:00+03:00",
            "airlines": "Emirates|American Airlines|Lufthansa|Ethiopian Airlines|Air India|Qatar Airways|Qatar Airways|Thai AirAsia|Turkish Airlines|LAM|United Airlines|Eurowings|American Airlines|Emirates|Etihad Airways|Thai AirAsia|Turkish Airlines|United Airlines|Qatar Airways|Turkish Airlines|Qatar Airways|LAM|Turkish Airlines|Asiana Airlines|Ethiopian Airlines|Myanmar Airways International|Qatar Airways|Ryanair|Ryanair|Qatar Airways|Qatar Airways|Cargolux Airlines International|Cargolux Airlines International|Cargolux Airlines International|Cargolux Airlines International"
        },
        {
            "flight_type": "J",
            "origin_iata": "FCO",
            "origin_country": "Italy",
            "destination_iata": "NBO",
            "destination_country": "Kenya",
            "num_stops": "7",
            "duration_hours": "140.83",
            "departure_dt": "2025-04-16 22:10:00+02:00",
            "arrival_dt": "2025-04-22 20:00:00+03:00",
            "airlines": "Emirates|Ryanair|Ryanair|Emirates|Emirates|Emirates|Emirates"
        }
    ],
    "totalPages": 1
}
```

## Logs and Debugging

- Logs for the Lambda function can be viewed in AWS CloudWatch.
- The deployment script logs its progress to the console.

## Notes

- Ensure your AWS credentials have sufficient permissions to create and manage Lambda, API Gateway, IAM roles, and Athena queries.
- Modify the Lambda function code (`lambda_function.py`) as needed for additional functionality.
