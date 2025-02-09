# Logz.io Metrics Reporter

## Overview
This repository contains:
- A Python script (`logz_metrics_handler.py`) that queries logs from different environments (e.g., EU and NA) based on specified parameters, processes them, and generates results in CSV format or uploads them to Confluence.
- A Jenkins pipeline (`Jenkinsfile`) that automates script execution and handles environment setup.
- A Dockerization option, including `docker-compose` support, to run the script in a containerized environment.
- Dependencies listed in `requirements.txt`.

The output, including `output.csv`, is generated as an artifact for every pipeline or container execution, and the details are optionally published to a Confluence page.

---

## Features
1. **Log Query and Processing**:
   - The script queries logs from `Logz.io` based on runtime parameters (e.g., date, time range, namespaces).
   - Tokens can be securely provided via arguments or environment variables set in a `.env` file.

2. **Output and Reporting**:
   - Generates a CSV file (`output.csv`) containing processed results.
   - Optionally creates a Confluence page for the metrics.

3. **Docker Support**:
   - Run the script easily in a containerized environment using either Docker or `docker-compose`, eliminating the need for manual environment setup.

4. **Pipeline Automation**:
   - The `Jenkinsfile` automates repository checkout, script execution, output generation, and artifact uploading.

5. **Sparse Checkout**:
   - Fetches only the necessary `customers.yml` file from an additional Git repository (`oas-deployment`) when required.

---

## Prerequisites

### To Run Locally:
- **Python 3.10+**
- `pip` for managing dependencies.

### On Docker or Docker Compose:
- **Docker** and **Docker Compose** installed.
- A `.env` file in the repository root containing the required tokens.

### On Jenkins:
1. Add the following credentials:
   - **Logzio_NA01_Token_SearchAPI**: Token for accessing NA logs.
   - **Logzio_EU01_Token_SearchAPI**: Token for accessing EU logs.
   - **CONFLUENCE_IMPORTER**: Credentials for Confluence.

2. Ensure Jenkins has Git configured to access this repository and optionally the `oas-deployment` repository.

---

## Repository Structure

```plaintext
.
├── Jenkinsfile                 # Jenkins pipeline definition
├── logz_metrics_handler.py     # Python script for querying logs
├── customers.yaml              # Openshift namespaces information
├── requirements.txt            # Python dependencies
├── Dockerfile                  # Docker configuration
├── docker-compose.yml          # Docker Compose setup
├── .env.example                # Example of the environment configuration file
├── README.md                   # Repository documentation
```

---

## Setup and Usage Instructions

### 1. **Run Locally**

#### Clone the Repository:
```bash
git clone git@git.cias.one:tid/logzio-metrics-reporter.git
cd logzio-metrics-reporter
```

#### Set Up Python Environment:
```bash
python3 -m venv venv
source venv/bin/activate   # For Windows: venv\Scripts\activate
pip install -r requirements.txt
```

#### Run the Script:
Execute the script by providing the required arguments:
```bash
python logz_metrics_handler.py \
    --platform "prd" \
    --date "2025-02-04" \
    --start_time "08:00:00Z" \
    --end_time "10:00:00Z" \
    --date_offset_range 14 \
    --eu_token "<EU_TOKEN>" \
    --na_token "<NA_TOKEN>" \
    --customers_file "customers.yml" \
    --page_title "Logz.io Metrics" \
    --confluence_username "<USERNAME>" \
    --confluence_password "<CONFLUENCE_PASSWORD>"
```

---

### 2. **Run with Docker**

#### Build Docker Image:
```bash
docker build -t logz-metrics-handler .
```

#### Run the Script in a Container:
```bash
docker run --rm logz-metrics-handler \
    --platform "prd" \
    --date "2025-02-04" \
    --start_time "08:00:00Z" \
    --end_time "10:00:00Z" \
    --date_offset_range 14 \
    --eu_token "<EU_TOKEN>" \
    --na_token "<NA_TOKEN>" \
    --customers_file "/app/customers.yaml" \
    --page_title "Logz.io Metrics" \
    --confluence_username "<CONFLUENCE_USERNAME>" \
    --confluence_password "<CONFLUENCE_PASSWORD>"
```

---

### 3. **Run with Docker Compose**

#### Set Up the Environment Variables:
1. Create a `.env` file in the repository root to define environment-specific variables required by Docker Compose.
   
   Example `.env` file:
   ```properties
   EU_API_TOKEN=your-eu-logz-token
   NA_API_TOKEN=your-na-logz-token
   CONFLUENCE_PASSWORD=your-confluence-password
   ```

   > **NOTE**: Do not commit the `.env` file, as it contains sensitive credentials.

2. You can use the provided `.env.example` file as a template for creating your `.env`.

#### Run the Application with Docker Compose:
```bash
docker compose up --build
```

This command will:
- Build the Docker image using the `Dockerfile`.
- Map environment variables from the `.env` file.
- Run the script with the specified parameters in the `docker-compose.yml` file.

#### Stopping the Service:
To stop the running container:
```bash
docker compose down
```

#### Customizing Docker Compose Command:
You can modify parameters in `docker-compose.yml` under the `command` field if needed, or pass arguments via environment variables.

---

### 4. **Run via Jenkins Pipeline**

#### Pipeline Overview:
The `Jenkinsfile` automates:
- Repository checkout (and optionally `oas-deployment` for `customers.yml`).
- Running the script with runtime parameters provided in the pipeline input.
- Capturing and archiving the output file (`output.csv`).

#### Trigger the Pipeline:
Provide the following parameters during pipeline execution:

| Parameter                 | Description                                                               |
|---------------------------|---------------------------------------------------------------------------|
| `PLATFORM`                | Target platform for queries (`prd` or `stg`).                             |
| `DATE`                    | Base date (`YYYY-MM-DD`).                                                 |
| `START_TIME`, `END_TIME`  | Start and End time in the format `HH:mm:ssZ` (UTC).                       |
| `DATE_OFFSET_RANGE`       | Time range for date-based processing.                                     |
| `CONFLUENCE_PAGE`         | Title of the page to create on Confluence.                                |
| `CHECKOUT_OAS_DEPLOYMENT` | Boolean flag to fetch the `customers.yml` file from `oas-deployment`.     |

The pipeline will automatically save the results (`output.csv`) as build artifacts.

---

## Common Errors and Troubleshooting

#### 1. Script Errors:
- **Error**: `FileNotFoundError: customers.yml not found`
  - Ensure the correct path to the `customers.yml` file is specified via `--customers_file`.

- **Error**: `Invalid API Token`
  - Check that the tokens provided for `EU` or `NA` are correct and have the required access.

#### 2. Docker Errors:
- **Error**: `Environment variable not defined`
  - Ensure all required keys are present in the `.env` file.

#### 3. Jenkins Pipeline Failures:
- **Error**: `Checkout failed for oas-deployment`
  - Verify the Jenkins job has access to the required Git repository.

---