# EDUARDO Project

EDUARDO (Enhanced Data Understanding and Analysis for Route Development Operations) is a data processing tool designed to collect, transform, and analyze flight data. It helps organizations track flight routes, analyze airline operations, and generate insights from aviation data. The project supports both local development with Docker and cloud deployment via AWS Lambda, with data stored in S3 and queryable through AWS Athena.

## Local Development with Docker

### Prerequisites for running locally

- Docker
- Docker Compose

### Setup

1. **Clone the repository**

   ```bash
   git clone <repository-url>

   ```

2. **Build and run with Docker Compose**

   ```bash
   docker-compose build
   ```

- **Run, retain results and don't upload to database**

  ```bash
  docker-compose run eduardo -c -d

  ```

- **Run in offline mode, retain files and don't upload to database**

  ```bash
  docker-compose run eduardo -o -c -d
  ```

## AWS Lambda Deployment

### Prerequisites for Deployment

- AWS CLI installed and configured
- An IAM role for Lambda execution (with S3 and CloudWatch permissions)
- AWS Account ID set in your environment
- Docker (for building dependencies)

### Environment Setup

Create a .env file with the variables from the .env.example

**IMPORTANT:** Remove the test suffixes from the function and layer names (check deploy.sh for more details) when deploying to production.

### Deployment Steps

1. Make the deployment script executable

   ```bash
   chmod +x deploy.sh

   ```

2. Run the deployment script

   ```bash
   ./deploy.sh
   ```

This script will:

- Build dependencies using a Lambda-compatible Docker container
- Create a Lambda Layer with all required libraries
- Package the Lambda function code
- Create or update the Lambda function
- Configure environment variables

### Testing the Deployed Lambda

1. Invoke the Lambda function

   ```bash
   aws lambda invoke \
   --function-name eduardo_get_data_test \
   --payload '{}' \
   response.json
   ```

2. Check the function logs

   ```bash
   aws logs filter-log-events \
   --log-group-name "/aws/lambda/eduardo_get_data_test" \
   --limit 10
   ```

3. Test using the AWS Console

   - Navigate to the AWS Lambda Console.
   - Select the `eduardo_get_data_test` function.
   - Use the "Test" feature to send any event payload.
   - The function takes approximately 10 minutes to run.
   - Upon completion, it generates both CSV and Parquet files.

**Note:** You can configure the Lambda's behavior by modifying the `config` object in `lambda_function.py`.

### Scheduled Execution

To configure the Lambda to run on a schedule:

1. Create an EventBridge rule

   ```bash
   aws events put-rule \
   --name "DailyEduardoRun" \
   --schedule-expression "cron(0 0 * * ? *)" \
   --description "Trigger EDUARDO daily at midnight"

   ```

2. Add permission for EventBridge to invoke Lambda

   ```bash
   aws lambda add-permission \
   --function-name eduardo_get_data_test \
   --statement-id EventBridgeInvoke \
   --action lambda:InvokeFunction \
   --principal events.amazonaws.com \
   --source-arn arn:aws:events:${AWS_REGION}:${AWS_ACCOUNT_ID}:rule/DailyEduardoRun

   ```

3. Connect the rule to the Lambda

   ```bash
   aws events put-targets \
   --rule DailyEduardoRun \
   --targets "Id"="1","Arn"="arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_ID}:function:eduardo_get_data_test"
   ```

## Athena Table

1. Make the athena setup script executable

   ```bash
   chmod +x setup_glue_athena.sh
   ```

2. Run the deployment script

   ```bash
   ./setup_glue_athena.sh
   ```

This script will:

- Load environment variables from the .env file
- Check if the table already exists
- Create a Glue database called `eduardo_flight_db_test`
- Create an IAM role and policy for Glue with appropriate permissions
- Create the AWS Glue crawler to discover the schema
- Set up an Athena table pointing to the S3 data location
- Configure table properties for optimal querying
- Generate sample Athena queries for testing and analysis:
  - `get_routes_between_countries.sql`
  - `get_flight_details_between_countries.sql`
  - `analyze_airline_data.sql`

### Table Definition

The table is created with the name `parquet` in the database `eduardo_flight_db_test`. Below is an example query to describe the table:

```sql
DESCRIBE eduardo_flight_db_test.parquet;
```

### Querying the Data with Athena

Once the setup is complete, you can query the data using the AWS Athena console or API:

1. Through AWS Console:

   - Navigate to the Athena console
   - Select the `eduardo_flight_db_test` database
   - Run queries against the `parquet` table

2. Sample Queries

   - **Get all routes between two countries**: Refer to `get_routes_between_countries.sql`
   - **Get flight details between two countries**: Refer to `get_flight_details_between_countries.sql`
   - **Analyze airline data**: Refer to `analyze_airline_data.sql`

   Example of running a query via AWS CLI:

   ```bash
   aws athena start-query-execution \
   --query-string "$(cat get_routes_between_countries.sql)" \
   --query-execution-context Database=eduardo_flight_db_test \
   --result-configuration OutputLocation="s3://logcluster-eduardo-bucket-test/athena-query-results/"
   ```

3. To manually trigger a Glue crawler:

   ```bash
   aws glue start-crawler --name "eduardo-flight-data-crawler"
   ```

4. To modify the table structure:

   ```bash
   aws glue update-table --database-name eduardo_flight_db_test --table-input file://updated_table_definition.json
   ```

### Maintenance and Updates

- The Glue crawler is configured to run daily to keep the schema updated.
- New data uploaded to the S3 bucket will be automatically discovered.
- Sample queries are saved in the project directory for easy access and testing.
