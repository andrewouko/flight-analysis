#!/bin/bash

# Disable AWS CLI pager output to avoid required manual intervention
export AWS_PAGER=""

set -e

# Check for AWS credentials
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo "❌ AWS credentials not configured. Please configure them using 'aws configure'."
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

LAMBDA_NAME="flight-query-lambda-test2"
ZIP_FILE="lambda_function.zip"
API_NAME="FlightQueryAPI_Test2"
ROLE_NAME="flight_query_lambda_execution_role_test"

# Package the Lambda function
echo "Packaging Lambda function..."
zip -j $ZIP_FILE lambda_function.py

# Create the Lambda execution role if it doesn't exist
echo "Ensuring Lambda execution role exists..."
ROLE_ARN=$(aws iam get-role --role-name $ROLE_NAME --query "Role.Arn" --output text 2>/dev/null || true)
if [ -z "$ROLE_ARN" ]; then
    echo "Creating new IAM role: $ROLE_NAME..."
    TRUST_POLICY=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF
)
    aws iam create-role --role-name $ROLE_NAME --assume-role-policy-document "$TRUST_POLICY"
    ROLE_ARN=$(aws iam get-role --role-name $ROLE_NAME --query "Role.Arn" --output text)
fi

# Attach necessary policies to the role
echo "Attaching policies to the role..."
aws iam attach-role-policy \
    --role-name $ROLE_NAME \
    --policy-arn arn:aws:iam::aws:policy/AmazonAthenaFullAccess
aws iam attach-role-policy \
    --role-name $ROLE_NAME \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Add S3 access policy for Athena results
echo "Adding S3 access policy for Athena results..."
S3_POLICY=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject", 
                "s3:ListBucket",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::logcluster-eduardo-bucket",
                "arn:aws:s3:::logcluster-eduardo-bucket/*",
                "arn:aws:s3:::$S3_BUCKET",
                "arn:aws:s3:::$S3_BUCKET/*"
            ]
        }
    ]
}
EOF
)

# Create custom policy for S3 access
aws iam put-role-policy \
    --role-name $ROLE_NAME \
    --policy-name "${LAMBDA_NAME}-s3-access" \
    --policy-document "$S3_POLICY"

# Create or update the Lambda function
if aws lambda get-function --function-name $LAMBDA_NAME > /dev/null 2>&1; then
    echo "Updating existing Lambda function..."
    aws lambda update-function-code --function-name $LAMBDA_NAME --zip-file fileb://$ZIP_FILE

    # Wait for the update to complete
    echo "Waiting for Lambda function to update..."
    aws lambda wait function-updated --function-name $LAMBDA_NAME
    
    # Update environment variables
    aws lambda update-function-configuration \
        --function-name $LAMBDA_NAME \
        --environment Variables="{ATHENA_DATABASE=$GLUE_DB_NAME,ATHENA_TABLE=$GLUE_TABLE_NAME,ATHENA_OUTPUT_LOCATION=s3://$S3_BUCKET/athena-results/}"
else
    echo "Creating new Lambda function..."
    aws lambda create-function \
        --function-name $LAMBDA_NAME \
        --runtime python3.9 \
        --role $ROLE_ARN \
        --handler lambda_function.handler \
        --zip-file fileb://$ZIP_FILE \
        --environment Variables="{ATHENA_DATABASE=$GLUE_DB_NAME,ATHENA_TABLE=$GLUE_TABLE_NAME,ATHENA_OUTPUT_LOCATION=s3://$S3_BUCKET/athena-results/}"
fi

# Create or update the API Gateway
API_ID=$(aws apigateway get-rest-apis --query "items[?name=='$API_NAME'].id" --output text)
if [ -z "$API_ID" ]; then
    echo "Creating new API Gateway..."
    API_ID=$(aws apigateway create-rest-api --name $API_NAME --query "id" --output text)
fi

# Get root resource ID
RESOURCE_ID=$(aws apigateway get-resources --rest-api-id $API_ID --query "items[?path=='/'].id" --output text)

# Check if GET method already exists for the resource
echo "Checking if GET method already exists..."
set +e
GET_METHOD=$(aws apigateway get-method --rest-api-id $API_ID --resource-id $RESOURCE_ID --http-method GET 2>/dev/null)
METHOD_EXISTS=$?
set -e

if [ $METHOD_EXISTS -ne 0 ]; then
    echo "Creating GET method for API Gateway..."
    aws apigateway put-method \
        --rest-api-id $API_ID \
        --resource-id $RESOURCE_ID \
        --http-method GET \
        --authorization-type "NONE"
    
    echo "Setting up integration for GET method..."
    aws apigateway put-integration \
        --rest-api-id $API_ID \
        --resource-id $RESOURCE_ID \
        --http-method GET \
        --type AWS_PROXY \
        --integration-http-method POST \
        --uri arn:aws:apigateway:$(aws configure get region):lambda:path/2015-03-31/functions/$(aws lambda get-function --function-name $LAMBDA_NAME --query "Configuration.FunctionArn" --output text)/invocations
else
    echo "GET method already exists, updating integration..."
    aws apigateway put-integration \
        --rest-api-id $API_ID \
        --resource-id $RESOURCE_ID \
        --http-method GET \
        --type AWS_PROXY \
        --integration-http-method POST \
        --uri arn:aws:apigateway:$(aws configure get region):lambda:path/2015-03-31/functions/$(aws lambda get-function --function-name $LAMBDA_NAME --query "Configuration.FunctionArn" --output text)/invocations
fi

# Deploy the API
echo "Deploying API Gateway..."
aws apigateway create-deployment --rest-api-id $API_ID --stage-name test

# Enable CloudWatch Logs for API Gateway
echo "Enabling CloudWatch Logs for API Gateway..."
aws apigateway update-stage \
    --rest-api-id $API_ID \
    --stage-name test \
    --patch-operations op=replace,path=/*/*/logging/loglevel,value=INFO

# Add permission for API Gateway to invoke Lambda (if not already added)
echo "Adding permission for API Gateway to invoke Lambda..."
set +e
PERMISSION_CHECK=$(aws lambda get-policy --function-name $LAMBDA_NAME 2>/dev/null)
PERMISSION_CHECK_STATUS=$?

# If policy exists, check if our statement is already there
if [ $PERMISSION_CHECK_STATUS -eq 0 ] && echo "$PERMISSION_CHECK" | grep -q "apigateway-invoke-permission"; then
    echo "Lambda permission for API Gateway already exists."
else
    echo "Adding new Lambda permission for API Gateway..."
    aws lambda add-permission \
        --function-name $LAMBDA_NAME \
        --statement-id apigateway-invoke-permission \
        --action lambda:InvokeFunction \
        --principal apigateway.amazonaws.com \
        --source-arn "arn:aws:execute-api:$(aws configure get region):$(aws sts get-caller-identity --query Account --output text):$API_ID/test/GET/"
fi
set -e

# Output the API Gateway URL
echo ""
echo "============================================="
echo "Deployment completed successfully!"
echo "============================================="
echo ""
echo "API Gateway URL:"
echo "https://$API_ID.execute-api.$(aws configure get region).amazonaws.com/test"
echo ""
echo "Test the API with:"
echo "curl https://$API_ID.execute-api.$(aws configure get region).amazonaws.com/test?origin=Italy&destination=Kenya"
echo ""
echo "Or visit in your browser:"
echo "https://$API_ID.execute-api.$(aws configure get region).amazonaws.com/test?origin=Italy&destination=Kenya"
echo "============================================="
