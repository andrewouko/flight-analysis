# Use Python 3.11 as base image (same as AWS Lambda environment)
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app:${PYTHONPATH}"

# Install system dependencies required for pandas and lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev \
    libxslt-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install development tools 
RUN pip install --no-cache-dir ipython pytest pytest-cov black flake8

# Copy application code
COPY . .

# Command to run the application
ENTRYPOINT ["python", "main.py"]