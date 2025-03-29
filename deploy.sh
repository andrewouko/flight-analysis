#!/bin/bash

# Disable AWS CLI pager output to avoid required manual intervention
export AWS_PAGER=""

set -e # Exit on any error

echo "=== EDUARDO Lambda Deployment ==="

# Cleanup function for removing temporary files
cleanup() {
    local keep_zip=${1:-false}
    echo "=== Cleaning up temporary files ==="
    rm -rf custom_layer python
    rm -f eduardo-custom-layer.zip ${LAYER_NAME}_result.json ${LAYER_NAME}_version.txt custom-requirements.txt
    
    # Optionally remove the function zip file
    if [ "$keep_zip" != "true" ]; then
        rm -f eduardo-function.zip
        echo "✅ All temporary files removed"
    else
        echo "✅ Temporary files removed (keeping eduardo-function.zip for future use)"
    fi
}

# Start on a clean slate
cleanup false  # Remove everything including the zip file

# Check for required commands
for cmd in aws zip docker grep cut; do
    if ! command -v $cmd &>/dev/null; then
        echo "❌ Required command '$cmd' not found. Please install it."
        exit 1
    fi
done

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

# Check for required variables
for var in S3_BUCKET FILE_NAME; do
    if [ -z "${!var}" ]; then
        echo "❌ Required variable $var not set in .env"
        exit 1
    fi
done

# Verify AWS credentials
echo "=== Verifying AWS Credentials ==="
IDENTITY=$(aws sts get-caller-identity --output json 2>&1)
if [ $? -ne 0 ]; then
    echo "❌ AWS credentials verification failed:"
    echo "${IDENTITY}"
    exit 1
fi
echo "✅ AWS credentials verified. You are authenticated as:"
echo "${IDENTITY}" | grep "Arn"

# Extract AWS account ID for S3 bucket naming
AWS_ACCOUNT_ID=$(echo "$IDENTITY" | grep -o '"Account": "[0-9]*"' | cut -d'"' -f4)

# Check if a Lambda execution role exists or create one
echo "=== Checking Lambda execution role ==="
ROLE_NAME="eduardo-lambda-execution-role"
LAMBDA_ROLE="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${ROLE_NAME}"

# Check if role exists
if ! aws iam get-role --role-name "${ROLE_NAME}" &>/dev/null; then
    echo "Creating Lambda execution role: ${ROLE_NAME}"

    # Create trust policy document
    cat >/tmp/lambda-trust-policy.json <<EOF
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

    # Create role with trust policy
    aws iam create-role \
        --role-name "${ROLE_NAME}" \
        --assume-role-policy-document file:///tmp/lambda-trust-policy.json \
        --description "Execution role for Eduardo Lambda functions" \
        --output json >/dev/null

    # Attach the AWSLambdaBasicExecutionRole policy for CloudWatch Logs
    aws iam attach-role-policy \
        --role-name "${ROLE_NAME}" \
        --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"

    # Give time for the role to propagate in AWS systems
    echo "Waiting for role to propagate (10 seconds)..."
    sleep 10
else
    echo "✅ Using existing role: ${ROLE_NAME}"
fi

# Update the LAMBDA_ROLE variable with the created/existing role
LAMBDA_ROLE="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${ROLE_NAME}"
echo "Using role ARN: ${LAMBDA_ROLE}"

# Add S3 permissions regardless of whether the role was just created or already existed
echo "Ensuring S3 access for Lambda role..."
# Create S3 access policy for the specific bucket
cat >/tmp/s3-access-policy.json <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::${S3_BUCKET}",
                "arn:aws:s3:::${S3_BUCKET}/*"
            ]
        }
    ]
}
EOF

# Create policy if it doesn't exist, or get existing ARN
POLICY_NAME="eduardo-lambda-s3-access"
set +e  # Disable exit on error temporarily
POLICY_ARN=$(aws iam create-policy \
    --policy-name ${POLICY_NAME} \
    --policy-document file:///tmp/s3-access-policy.json \
    --query 'Policy.Arn' \
    --output text 2>/dev/null || \
    aws iam list-policies --scope Local --query "Policies[?PolicyName=='${POLICY_NAME}'].Arn" --output text)
set -e  # Re-enable exit on error

# Attach policy to role (this is idempotent - won't fail if already attached)
aws iam attach-role-policy \
    --role-name "${ROLE_NAME}" \
    --policy-arn "${POLICY_ARN}"
echo "✅ S3 access policy attached to role"
echo "✅ S3 access policy ARN: ${POLICY_ARN}"

# Lambda function and layer configuration variables
FUNCTION_NAME=${FUNCTION_NAME:-"eduardo_get_data_test"}
REGION=${AWS_REGION:-"eu-west-1"}
LAYER_NAME=${LAYER_NAME:-"eduardo_data_layer_test"}
LAMBDA_MEMORY=${LAMBDA_MEMORY:-1024}                      # Memory in MB
LAMBDA_EPHEMERAL_STORAGE=${LAMBDA_EPHEMERAL_STORAGE:-512} # Storage in MB
LAMBDA_TIMEOUT=${LAMBDA_TIMEOUT:-600}                     # Timeout in seconds
AWS_PANDAS_LAYER_ARN="arn:aws:lambda:${REGION}:336392948345:layer:AWSSDKPandas-Python311:5"

# Function environment variables string for reuse
ENV_VARS="S3_BUCKET=${S3_BUCKET},S3_KEY=${S3_KEY},FILE_NAME=${FILE_NAME},ORIGINS_AND_DESTINATIONS_SHEET=${ORIGINS_AND_DESTINATIONS_SHEET},COUNTRIES_SHEET=${COUNTRIES_SHEET},PARAMETERS_SHEET=${PARAMETERS_SHEET},FLIGHT_TYPES_SHEET=${FLIGHT_TYPES_SHEET},AIRPORTS_SHEET=${AIRPORTS_SHEET},DB_DATABASE=${DB_DATABASE},DB_HOST=${DB_HOST},DB_PASS=${DB_PASS},DB_PROTOCOL=${DB_PROTOCOL},DB_TABLE=${DB_TABLE},DB_USER=${DB_USER}"

# Build only non-AWS dependencies
echo "=== Step 1: Building custom Lambda-compatible dependencies ==="
docker run --rm --entrypoint /bin/bash -v "$PWD":/var/task public.ecr.aws/lambda/python:3.11 \
    -c "yum install -y gcc gcc-c++ python3-devel && \
        pip install --upgrade pip && \
        pip install --no-deps --no-cache-dir --platform manylinux2014_x86_64 --only-binary=:all: \
        -r filtered-requirements.txt -t python/lib/python3.11/site-packages/ && \
        find python -name \"__pycache__\" -type d -exec rm -rf {} + 2>/dev/null || true && \
        ls -l python/lib/python3.11/site-packages/ && echo 'PACKAGES INSTALLED SUCCESSFULLY'"
echo "✅ Custom Lambda-compatible dependencies installed"

# Helper function to create and deploy the single custom layer
create_and_deploy_custom_layer() {
    # In create_and_deploy_custom_layer
    echo "=== Analyzing Package Sizes ==="
    du -h python/lib/python3.11/site-packages/* | sort -hr
    # Total size of all packages
    TOTAL_SIZE=$(du -sh python/lib/python3.11/site-packages | cut -f1)
    echo "Total size: ${TOTAL_SIZE}"

    echo "=== Creating Custom Lambda Layer ==="

    # Create the base directory structure for the layer
    mkdir -p custom_layer/python/lib/python3.11/site-packages
    # Copy with exclusion of AWS-provided packages
    echo "Copying required packages (excluding AWS-provided)..."
    for PKG_DIR in python/lib/python3.11/site-packages/*; do
        if [ -d "$PKG_DIR" ]; then
            PKG_NAME=$(basename "$PKG_DIR")
            
            # More thorough check with explicit patterns 
            if [[ "$PKG_NAME" == "numpy" || "$PKG_NAME" == "pandas" || "$PKG_NAME" == "boto3" || 
                "$PKG_NAME" == "botocore" || "$PKG_NAME" == "pytz" || "$PKG_NAME" == "six" ||
                "$PKG_NAME" == "python-dateutil" || "$PKG_NAME" == "python_dateutil" || 
                "$PKG_NAME" == "dateutil" || "$PKG_NAME" == "s3transfer" || "$PKG_NAME" == "jmespath" ||
                "$PKG_NAME" == *"numpy"* || "$PKG_NAME" == *"pandas"* || 
                "$PKG_NAME" == *"-numpy-"* || "$PKG_NAME" == *"-pandas-"* ]]; then
                echo "  ⚠️ Skipping AWS-provided package: ${PKG_NAME}"
            else
                cp -r "$PKG_DIR" custom_layer/python/lib/python3.11/site-packages/
                echo "  ✅ Copied ${PKG_NAME}"
            fi
        fi
    done

    # Final sanity check for any numpy/pandas files (use -delete instead of -exec rm)
    find custom_layer -name "*numpy*" -type d -delete 2>/dev/null || true
    find custom_layer -name "*pandas*" -type d -delete 2>/dev/null || true
    find custom_layer -path "*/site-packages/numpy*" -delete 2>/dev/null || true
    find custom_layer -path "*/site-packages/pandas*" -delete 2>/dev/null || true
    # Special case: Remove tqdm_pandas.py file that can cause conflicts
    find custom_layer -name "*tqdm_pandas.py" -delete 2>/dev/null || true

    # Debug: confirm no numpy/pandas related files remain
    echo "=== Confirming no numpy/pandas remains ==="
    NUMPY_FILES=$(find custom_layer -name "*numpy*" 2>/dev/null)
    if [ -z "$NUMPY_FILES" ]; then
        echo "No numpy found (good!)"
    else
        echo "⚠️ Found numpy files: $NUMPY_FILES"
    fi

    PANDAS_FILES=$(find custom_layer -name "*pandas*" 2>/dev/null)
    if [ -z "$PANDAS_FILES" ]; then
        echo "No pandas found (good!)"
    else
        echo "⚠️ Found pandas files: $PANDAS_FILES"
    fi

    # Create the layer zip file
    cd custom_layer && zip -r ../eduardo-custom-layer.zip python >/dev/null && cd ..

    # Deploy the layer
    deploy_single_layer "eduardo-custom-layer.zip" "${LAYER_NAME}" "EDUARDO Custom Libraries"

    # Save version for reference
    echo "${LAYER_VERSION}" >${LAYER_NAME}_version.txt
}

# Helper function for deploying a single layer
deploy_single_layer() {
    ZIP_FILE=$1
    TARGET_LAYER_NAME=$2
    DESCRIPTION=$3

    echo "=== Validating Layer Size ==="

    # Cross-platform size check
    if [[ "$(uname)" == "Darwin" ]]; then
        LAYER_SIZE_BYTES=$(stat -f%z "$ZIP_FILE")
    else
        LAYER_SIZE_BYTES=$(stat -c%s "$ZIP_FILE")
    fi

    MAX_LAYER_BYTES=$((250*1024*1024 - 90*1024*1024))  # 250MB total - 90MB for AWS layer

    echo "Uncompressed Layer size: $(($LAYER_SIZE_BYTES/1024/1024))MB"
    echo "Max layer size: $(($MAX_LAYER_BYTES/1024/1024))MB"
    
    
    if [ $LAYER_SIZE_BYTES -gt $MAX_LAYER_BYTES ]; then
        echo "❌ Layer too large: $(($LAYER_SIZE_BYTES/1024/1024))MB > $(($MAX_LAYER_BYTES/1024/1024))MB"
        echo "Reduce dependencies or split into multiple layers"
        exit 1
    fi

    echo "=== Deploying ${TARGET_LAYER_NAME} ==="
    LAYER_SIZE=$(du -h ${ZIP_FILE} | cut -f1)
    echo "Layer size: ${LAYER_SIZE}"

    # Upload layer to S3
    LAYER_KEY="lambda-layers/${TARGET_LAYER_NAME}-$(date +%s).zip"
    echo "Uploading layer to s3://${S3_BUCKET}/${LAYER_KEY}..."
    aws s3 cp ${ZIP_FILE} s3://${S3_BUCKET}/${LAYER_KEY}

    if [ $? -eq 0 ]; then
        echo "✅ Layer uploaded to S3 successfully"

        # Create layer from S3 location
        aws lambda publish-layer-version \
            --layer-name ${TARGET_LAYER_NAME} \
            --description "${DESCRIPTION}" \
            --content S3Bucket=${S3_BUCKET},S3Key=${LAYER_KEY} \
            --compatible-runtimes "python3.11" \
            --output json >${TARGET_LAYER_NAME}_result.json

        if [ $? -eq 0 ]; then
            LAYER_VERSION=$(cat ${TARGET_LAYER_NAME}_result.json | grep '"Version":' | cut -d':' -f2 | tr -d ',' | tr -d ' ')
            echo "✅ Lambda Layer created: ${TARGET_LAYER_NAME}:${LAYER_VERSION}"
            export LAYER_VERSION="${LAYER_VERSION}"
        else
            echo "❌ Failed to create layer from S3."
            echo "ERROR DETAILS:"
            cat ${TARGET_LAYER_NAME}_result.json
            exit 1
        fi
    else
        echo "❌ Failed to upload layer to S3."
        exit 1
    fi
}

# Split packages into multiple Lambda layers
echo "=== Step 2: Creating and deploying Lambda layers ==="
create_and_deploy_custom_layer

# Store versions in variables for reporting
SAVED_CUSTOM_VERSION=$(cat ${LAYER_NAME}_version.txt 2>/dev/null || echo "?")

echo "${LAYER_NAME} layer version: ${SAVED_CUSTOM_VERSION}"
echo "Using AWS SDKPandas layer: ${AWS_PANDAS_LAYER_ARN}"

# Package the function
echo "=== Step 3: Packaging Lambda function ==="
# List of all potential project directories
POTENTIAL_DIRS=("models" "utils" "services" "lib" "helpers")
ZIP_CMD="zip -r eduardo-function.zip *.py"

# Add each directory that exists
for DIR in "${POTENTIAL_DIRS[@]}"; do
    if [ -d "$DIR" ]; then
        echo "Including directory: $DIR"
        ZIP_CMD="$ZIP_CMD $DIR/*.py"
    fi
done

# Execute the constructed zip command
if ! eval "$ZIP_CMD > /dev/null"; then
    echo "❌ Failed to package Lambda function code"
    exit 1
fi
echo "✅ Function code packaged"

# Check if function exists - either update or create
echo "=== Step 4: Deploying Lambda function ==="
echo "Checking if function ${FUNCTION_NAME} exists..."

# Temporarily disable exit on error for the function check
set +e
FUNCTION_CHECK_RESULT=$(aws lambda get-function --function-name ${FUNCTION_NAME} 2>&1)
FUNCTION_EXISTS=$?
# Re-enable exit on error
set -e

# Debug output
echo "Function check result code: ${FUNCTION_EXISTS}"

if [ $FUNCTION_EXISTS -eq 0 ]; then
    echo "🔄 Function exists - updating configuration and code"

    # Update function code first
    if ! aws lambda update-function-code \
        --function-name ${FUNCTION_NAME} \
        --zip-file fileb://eduardo-function.zip; then
        echo "❌ Failed to update function code"
        exit 1
    fi

    # Wait for function update to complete
    echo "Waiting for function code update to complete..."
    retry_count=0
    max_retries=10
    while [ $retry_count -lt $max_retries ]; do
        sleep 5
        # Check function state
        STATE=$(aws lambda get-function --function-name ${FUNCTION_NAME} --query 'Configuration.State' --output text)
        if [ "$STATE" == "Active" ]; then
            break
        fi
        echo "Function state is $STATE, waiting..."
        retry_count=$((retry_count + 1))
    done

    if [ $retry_count -eq $max_retries ]; then
        echo "⚠️ Timed out waiting for function to become active, but proceeding anyway..."
    fi

    # Update function configuration
    if ! aws lambda update-function-configuration \
        --function-name ${FUNCTION_NAME} \
        --layers \
        "${AWS_PANDAS_LAYER_ARN}" \
        "arn:aws:lambda:${REGION}:${AWS_ACCOUNT_ID}:layer:${LAYER_NAME}:${SAVED_CUSTOM_VERSION}" \
        --timeout ${LAMBDA_TIMEOUT} \
        --memory-size ${LAMBDA_MEMORY} \
        --ephemeral-storage Size=${LAMBDA_EPHEMERAL_STORAGE} \
         --output json \
        --environment "Variables={${ENV_VARS}}"; then
        echo "❌ Failed to update function configuration"
        exit 1
    fi

    echo "✅ Lambda function updated successfully"
else
    # Check if this is a permission error
    if echo "$FUNCTION_CHECK_RESULT" | grep -q "AccessDenied"; then
        echo "❌ AWS permission error: You don't have sufficient permissions to check Lambda functions"
        echo "$FUNCTION_CHECK_RESULT"
        exit 1
    elif echo "$FUNCTION_CHECK_RESULT" | grep -q "ResourceNotFoundException"; then
        echo "🆕 Function doesn't exist - creating new function"
        if ! aws lambda create-function \
            --function-name ${FUNCTION_NAME} \
            --runtime python3.11 \
            --role ${LAMBDA_ROLE} \
            --handler lambda_function.lambda_handler \
            --timeout ${LAMBDA_TIMEOUT} \
            --memory-size ${LAMBDA_MEMORY} \
            --ephemeral-storage Size=${LAMBDA_EPHEMERAL_STORAGE} \
            --zip-file fileb://eduardo-function.zip \
            --environment "Variables={${ENV_VARS}}" \
             --output json \
            --layers \
                "arn:aws:lambda:${REGION}:${AWS_ACCOUNT_ID}:layer:${LAYER_NAME}:${SAVED_CUSTOM_VERSION}" \
                "${AWS_PANDAS_LAYER_ARN}"; then

            echo "❌ Failed to create Lambda function"
            exit 1
        fi
        echo "✅ Lambda function created successfully"
    else
        # Some other unexpected error
        echo "❌ Unexpected error checking Lambda function:"
        echo "$FUNCTION_CHECK_RESULT"
        exit 1
    fi
fi

# Upload Excel file to S3 if it exists
echo "=== Step 5: Uploading Excel file to S3 ==="
if [ -f "${FILE_NAME}" ]; then
    echo "Uploading ${FILE_NAME} to s3://${S3_BUCKET}/${S3_KEY}${FILE_NAME}..."

    if ! aws s3 cp "${FILE_NAME}" "s3://${S3_BUCKET}/${S3_KEY}${FILE_NAME}"; then
        echo "❌ Failed to upload Excel file to S3"
    else
        echo "✅ Excel file uploaded to S3 successfully"
    fi
else
    echo "⚠️ Excel file ${FILE_NAME} not found, skipping upload"
fi

# Clean up temporary files
cleanup true

# Report successful deployment
echo "=== Deployment completed successfully! ==="
echo "Function: ${FUNCTION_NAME}"
echo "Layers:"
echo "  - AWS SDKPandas: ${AWS_PANDAS_LAYER_ARN}"
echo "  - ${LAYER_NAME}:${SAVED_CUSTOM_VERSION}"
echo "Memory: ${LAMBDA_MEMORY}MB"
echo "Ephemeral storage: ${LAMBDA_EPHEMERAL_STORAGE}MB"
echo "Timeout: $((${LAMBDA_TIMEOUT} / 60))min $((${LAMBDA_TIMEOUT} % 60))sec"
