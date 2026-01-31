FROM python:3.11-slim-bookworm

# Metadata
LABEL maintainer="Data Engineering Pipeline"
LABEL description="Orchestrated environment for crypto-asset ETL pipelines"

# Environment Settings
# - PYTHONUNBUFFERED: Forces stdout/stderr to be flushed immediately
# - PYTHONDONTWRITEBYTECODE: Prevents .pyc files from cluttering the image
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# Set the working directory
WORKDIR /app

# System Dependencies
# Installed in a single layer to reduce image size.
# 'build-essential' and 'git' included for compiling C-extensions in pandas/numpy if needed.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Layer Caching Strategy:
# Copy only requirements first. This ensures that if only the code changes,
# Docker doesn't re-install all Python packages.
COPY requirements.txt .

# Install Python Dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy the Application Code
COPY . .

# Creates a non-root user.
RUN useradd -m pipeline_user && \
    chown -R pipeline_user:pipeline_user /app

# Switch to non-root user
USER pipeline_user

# Make the entrypoint executable
RUN chmod +x run_pipelines.sh

# Entrypoint
CMD ["./run_pipelines.sh"]