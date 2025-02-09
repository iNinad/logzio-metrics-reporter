# Use a lightweight Python 3.10 base image
FROM python:3.10-slim

# Create a working directory in the container
WORKDIR /app

# Copy the requirements file to the container
COPY requirements.txt requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script and any associated files to the container
COPY . .

# Default command to run the Python application
ENTRYPOINT ["python", "logz_metrics_handler.py"]