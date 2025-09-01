FROM python:3.11-slim

# Install system dependencies including ODBC drivers
RUN apt-get update && apt-get install -y \
    gnupg2 \
    curl \
    && apt-get update \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Run the script
CMD ["./run_exports.sh"]