#!/bin/bash

# Disable AWS CLI pager output to avoid required manual intervention
export AWS_PAGER=""

# Exit on any error
set -e

# Cleanup function for removing temporary files
cleanup() {
    ONLY_TEMP_FILES=$1

    echo "Cleaning up temporary files..."
    rm -f /tmp/glue-trust-policy.json /tmp/s3-access-policy.json /tmp/create-table.sql /tmp/athena-workgroup.json
    
    if [ "$ONLY_TEMP_FILES" != "true" ]; then
        # Remove the test query files
        echo "Removing test query files..."
        rm -f test_athena_query.sql
        rm -f get_routes_between_countries.sql
        rm -f get_flight_details_between_countries.sql
        rm -f analyze_airline_data.sql

    fi

    echo "Temporary files cleaned up."
}

# Clean slate
cleanup

# Load environment variables from .env
if [ -f .env ]; then
    # Use source command to load the environment file
    set -a # automatically export all variables
    source .env
    set +a # disable auto-export

    # Basic AWS configuration from .env file
    export AWS_DEFAULT_REGION="${AWS_REGION:-eu-west-1}"

    echo "✅ Environment variables loaded from .env"
else
    echo "❌ .env file not found."
    exit 1
fi

# Set variables with defaults
S3_BUCKET=${S3_BUCKET:-"logcluster-eduardo-bucket-test"}
AWS_REGION=${AWS_REGION:-"eu-west-1"}
GLUE_DB_NAME=${GLUE_DB_NAME:-"eduardo_flight_db_test"}
GLUE_TABLE_NAME=${GLUE_TABLE_NAME:-"parquet"}
GLUE_ROLE_NAME=${GLUE_ROLE_NAME:-"AWSGlueServiceRole-EduardoCrawlerRole"}
CRAWLER_NAME=${CRAWLER_NAME:-"eduardo-flight-data-crawler"}
S3_PATH=${S3_PATH:-"eduardo/datasets/parquet/"}
GLUE_POLICY_NAME=${GLUE_POLICY_NAME:-"EduardoS3AccessPolicy"}
ATHENA_RESULTS_PATH=${ATHENA_RESULTS_PATH:-"athena-query-results/"}
ATHENA_WORKGROUP=${ATHENA_WORKGROUP:-"eduardo-workgroup-test"}

echo "========== EDUARDO Glue/Athena Setup =========="
echo "S3 Bucket: $S3_BUCKET"
echo "AWS Region: $AWS_REGION"
echo "Glue Database: $GLUE_DB_NAME"
echo "Glue Table: $GLUE_TABLE_NAME"
echo "S3 Path: $S3_PATH"
echo "Athena Results Path: $ATHENA_RESULTS_PATH"
echo "Athena Workgroup: $ATHENA_WORKGROUP"
echo "=============================================="

# Get AWS account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
echo "AWS Account ID: $AWS_ACCOUNT_ID"

# Step 1: Create the Glue database if it doesn't exist
echo "Creating Glue database $GLUE_DB_NAME if it doesn't exist..."
aws glue create-database --database-input "{\"Name\": \"$GLUE_DB_NAME\"}" 2>/dev/null || echo "Database already exists"

# Step 2: Create IAM role for Glue crawler
echo "Setting up IAM role for Glue crawler..."
# Check if role already exists
if aws iam get-role --role-name "$GLUE_ROLE_NAME" 2>/dev/null; then
    echo "Role $GLUE_ROLE_NAME already exists."
else
    # Create trust relationship document
    cat > /tmp/glue-trust-policy.json <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "glue.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF

    # Create the role
    aws iam create-role \
        --role-name "$GLUE_ROLE_NAME" \
        --assume-role-policy-document file:///tmp/glue-trust-policy.json
    
    # Attach AWSGlueServiceRole managed policy
    aws iam attach-role-policy \
        --role-name "$GLUE_ROLE_NAME" \
        --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
    
    echo "Created role $GLUE_ROLE_NAME"
fi

# Step 3: Create and attach S3 bucket access policy
echo "Setting up S3 access policy..."
# Create policy document
cat > /tmp/s3-access-policy.json <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::$S3_BUCKET",
                "arn:aws:s3:::$S3_BUCKET/*"
            ]
        }
    ]
}
EOF

# Create or get existing policy
set +e # Don't exit on error for this step
POLICY_ARN=$(aws iam create-policy \
    --policy-name "$GLUE_POLICY_NAME" \
    --policy-document file:///tmp/s3-access-policy.json \
    --query 'Policy.Arn' \
    --output text 2>/dev/null || \
    aws iam list-policies --scope Local --query "Policies[?PolicyName=='$GLUE_POLICY_NAME'].Arn" --output text)
set -e # Re-enable exit on error

# Attach policy to role if not already attached
if [ -n "$POLICY_ARN" ]; then
    # Check if policy is already attached to role
    set +e
    POLICY_ATTACHED=$(aws iam list-attached-role-policies --role-name "$GLUE_ROLE_NAME" \
        --query "AttachedPolicies[?PolicyArn=='$POLICY_ARN'].PolicyArn" --output text)
    set -e
    
    if [ -z "$POLICY_ATTACHED" ]; then
        aws iam attach-role-policy \
            --role-name "$GLUE_ROLE_NAME" \
            --policy-arn "$POLICY_ARN"
        echo "Attached S3 access policy to role"
    else
        echo "S3 access policy already attached to role"
    fi
else
    echo "Failed to create or find S3 access policy"
    exit 1
fi

# Step 4: Check crawler status before attempting to update
echo "Checking Glue crawler status..."
set +e
CRAWLER_EXISTS=$(aws glue get-crawler --name "$CRAWLER_NAME" 2>/dev/null)
CRAWLER_STATUS=$?
set -e

if [ $CRAWLER_STATUS -eq 0 ]; then
    echo "Crawler $CRAWLER_NAME already exists. Checking its state..."
    
    # Get current crawler state
    CRAWLER_STATE=$(aws glue get-crawler --name "$CRAWLER_NAME" --query 'Crawler.State' --output text)
    
    if [ "$CRAWLER_STATE" == "RUNNING" ]; then
        echo "Crawler is currently running. Cannot update configuration."
        echo "You can either:"
        echo "  1. Wait for the crawler to finish, or"
        echo "  2. Stop the crawler with: aws glue stop-crawler --name \"$CRAWLER_NAME\""
        echo "The rest of the setup is complete. Crawler will use existing configuration."
    else
        # Update crawler configuration
        echo "Updating crawler configuration for $CRAWLER_NAME..."
        aws glue update-crawler \
            --name "$CRAWLER_NAME" \
            --role "arn:aws:iam::$AWS_ACCOUNT_ID:role/$GLUE_ROLE_NAME" \
            --database-name "$GLUE_DB_NAME" \
            --targets "{\"S3Targets\":[{\"Path\":\"s3://$S3_BUCKET/$S3_PATH\"}]}" \
            --schedule "cron(0 0 * * ? *)" \
            --table-prefix "" \
            --schema-change-policy "{\"UpdateBehavior\":\"UPDATE_IN_DATABASE\",\"DeleteBehavior\":\"LOG\"}" \
            --recrawl-policy "{\"RecrawlBehavior\":\"CRAWL_EVERYTHING\"}" \
            --configuration "{\"Version\":1.0,\"CrawlerOutput\":{\"Partitions\":{\"AddOrUpdateBehavior\":\"InheritFromTable\"},\"Tables\":{\"AddOrUpdateBehavior\":\"MergeNewColumns\"}}}"
        
        # Start crawler if it's not already running
        echo "Starting Glue crawler..."
        aws glue start-crawler --name "$CRAWLER_NAME"
        echo "Crawler started. It may take a few minutes to complete."
    fi
else
    echo "Creating new Glue crawler $CRAWLER_NAME..."
    aws glue create-crawler \
        --name "$CRAWLER_NAME" \
        --role "arn:aws:iam::$AWS_ACCOUNT_ID:role/$GLUE_ROLE_NAME" \
        --database-name "$GLUE_DB_NAME" \
        --targets "{\"S3Targets\":[{\"Path\":\"s3://$S3_BUCKET/$S3_PATH\"}]}" \
        --schedule "cron(0 0 * * ? *)" \
        --table-prefix "" \
        --schema-change-policy "{\"UpdateBehavior\":\"UPDATE_IN_DATABASE\",\"DeleteBehavior\":\"LOG\"}" \
        --recrawl-policy "{\"RecrawlBehavior\":\"CRAWL_EVERYTHING\"}" \
        --configuration "{\"Version\":1.0,\"CrawlerOutput\":{\"Partitions\":{\"AddOrUpdateBehavior\":\"InheritFromTable\"},\"Tables\":{\"AddOrUpdateBehavior\":\"MergeNewColumns\"}}}"

    # Start the crawler
    echo "Starting Glue crawler..."
    aws glue start-crawler --name "$CRAWLER_NAME"
    echo "Crawler started. It may take a few minutes to complete."
fi

# Step 5: Create sample test query 
echo "Creating sample test query..."
cat > test_athena_query.sql <<EOF
-- Test query for EDUARDO flight data
SELECT 
  "flight type" AS flight_type,
  "origin iata" AS origin_iata,
  "origin country" AS origin_country,
  "destination iata" AS destination_iata, 
  "destination country" AS destination_country,
  "num stops" AS num_stops,
  CAST("duration" AS DOUBLE) AS duration_hours,
  CAST("departure dt" AS TIMESTAMP) AS departure_dt,
  CAST("arrival dt" AS TIMESTAMP) AS arrival_dt
FROM $GLUE_DB_NAME.$GLUE_TABLE_NAME
WHERE "origin country" = 'Italy' AND "destination country" = 'Kenya'
LIMIT 10;
EOF

# Step 6: Create sample queries
echo "Creating sample queries..."

# Query 1: Get all routes between two countries
cat > /Users/andrewouko/projects/wfp-work/EDUARDO/get_routes_between_countries.sql <<EOF
-- Get all routes between two countries
SELECT 
  "origin country" AS origin_country,
  "destination country" AS destination_country,
  COUNT(*) AS route_count
FROM $GLUE_DB_NAME.$GLUE_TABLE_NAME
WHERE "origin country" = 'Italy' AND "destination country" = 'Kenya'
GROUP BY "origin country", "destination country";
EOF

# Query 2: Get flight details between two countries
cat > /Users/andrewouko/projects/wfp-work/EDUARDO/get_flight_details_between_countries.sql <<EOF
-- Get flight details between two countries
SELECT 
  "flight type" AS flight_type,
  "origin iata" AS origin_iata,
  "origin country" AS origin_country,
  "destination iata" AS destination_iata, 
  "destination country" AS destination_country,
  "num stops" AS num_stops,
  CAST("duration" AS DOUBLE) AS duration_hours,
  CAST("departure dt" AS TIMESTAMP) AS departure_dt,
  CAST("arrival dt" AS TIMESTAMP) AS arrival_dt
FROM eduardo_flight_db_test.parquet
WHERE "origin country" = 'Italy' AND "destination country" = 'Kenya'
LIMIT 10;
EOF

# Query 3: Analyze airline data from pipe-delimited values
cat > /Users/andrewouko/projects/wfp-work/EDUARDO/analyze_airline_data.sql <<EOF
-- Query to analyze airline data from pipe-delimited values
WITH airline_data AS (
  -- Use UNNEST to split pipe-delimited airline names into separate rows
  SELECT 
    airline_name,
    TRY_CAST("duration" AS DOUBLE) AS duration
  FROM (
    -- First, split the airline names string into an array
    SELECT 
      SPLIT("airline name(s)", '|') AS airline_names,
      "duration"
    FROM $GLUE_DB_NAME.$GLUE_TABLE_NAME
    WHERE "duration" IS NOT NULL
    AND TRY_CAST("duration" AS DOUBLE) IS NOT NULL
  )
  CROSS JOIN UNNEST(airline_names) AS t(airline_name)
)

-- Now calculate statistics per airline
SELECT 
  airline_name AS airline,
  COUNT(*) AS flight_count,
  AVG(duration) AS avg_duration_hours,
  MIN(duration) AS min_duration_hours,
  MAX(duration) AS max_duration_hours
FROM airline_data
GROUP BY airline_name
HAVING COUNT(*) > 5  -- Only show airlines with sufficient data
ORDER BY avg_duration_hours DESC;
EOF

# Step 7: Create or update Athena workgroup with default query result location
echo "Setting up Athena workgroup with default query result location..."
# Create Athena workgroup configuration file
cat > /tmp/athena-workgroup.json <<EOF
{
  "Name": "$ATHENA_WORKGROUP",
  "Configuration": {
    "ResultConfiguration": {
      "OutputLocation": "s3://$S3_BUCKET/$ATHENA_RESULTS_PATH"
    },
    "EnforceWorkGroupConfiguration": true,
    "PublishCloudWatchMetricsEnabled": true,
    "EngineVersion": {
      "SelectedEngineVersion": "AUTO"
    }
  }
}
EOF

# Check if workgroup exists
set +e
WORKGROUP_EXISTS=$(aws athena get-work-group --work-group "$ATHENA_WORKGROUP" 2>/dev/null)
WORKGROUP_STATUS=$?
set -e

if [ $WORKGROUP_STATUS -eq 0 ]; then
    echo "Updating existing Athena workgroup $ATHENA_WORKGROUP..."
    aws athena update-work-group \
      --work-group "$ATHENA_WORKGROUP" \
      --configuration file:///tmp/athena-workgroup.json \
      --description "Workgroup for EDUARDO project with default query result location"
else
    echo "Creating new Athena workgroup $ATHENA_WORKGROUP..."
    aws athena create-work-group \
      --cli-input-json file:///tmp/athena-workgroup.json \
      --description "Workgroup for EDUARDO project with default query result location"
fi

echo "Athena workgroup configured with result location: s3://$S3_BUCKET/$ATHENA_RESULTS_PATH"

echo "Sample queries created:"
echo "  - get_routes_between_countries.sql"
echo "  - get_flight_details_between_countries.sql"
echo "  - analyze_airline_data.sql"

echo "========== Setup Complete =========="
echo "Database: $GLUE_DB_NAME"
echo "Table: $GLUE_TABLE_NAME"
echo "Crawler: $CRAWLER_NAME"
echo "Sample query saved to: test_athena_query.sql"
echo "To test the setup, run:"
echo "  aws athena start-query-execution \\"
echo "    --query-string \"\$(cat test_athena_query.sql)\" \\"
echo "    --query-execution-context Database=$GLUE_DB_NAME \\"
echo "    --work-group \"$ATHENA_WORKGROUP\""
echo ""
echo "Note: Wait for the crawler to finish before querying the data."
echo "Check crawler status: aws glue get-crawler --name \"$CRAWLER_NAME\" --query 'Crawler.State'"
echo "=================================="


# Cleanup temporary files
cleanup "true"