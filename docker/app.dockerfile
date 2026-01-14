# Use an official lightweight Python image.
# python:3.11-slim is standard for modern production environments.
FROM python:3.11-slim

# Set environment variables
# PYTHONDONTWRITEBYTECODE: Prevents Python from writing .pyc files to disc
# PYTHONUNBUFFERED: Prevents Python from buffering stdout/stderr (better logs)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies
# build-essential: Required for compiling certain Python packages
# libpq-dev: Required for psycopg2 (PostgreSQL adapter)
# curl: Useful for health checks
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file first to leverage Docker cache
# This ensures dependencies are only re-installed if requirements.txt changes
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Default command to keep the container running during development.
# In production, this would be replaced by the uvicorn start command.
CMD ["tail", "-f", "/dev/null"]