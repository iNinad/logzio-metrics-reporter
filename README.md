# Logz.io Metrics Reporter

## Overview
This repository contains:
- A Python script to query logs from different environments (e.g., EU and NA) based on specific parameters, fetching data efficiently.
- A Jenkins pipeline (`Jenkinsfile`) that automates the execution of the script.
- Dependencies listed in `requirements.txt` for setting up a virtual environment.
- Integration with the `oas-deployment` repository to fetch the `customers.yaml` configuration file using **sparse checkout**.

The pipeline generates `output.csv` containing processed log data, attached as a build artifact for every pipeline execution.

---

## Features
1. **Script Execution**: 
   - The Python script queries logs based on user-defined parameters such as date, start time, and end time.
   - It leverages tokens securely fetched from Jenkins credentials for NA and EU environments.

2. **Customers File Management**:
   - `customers.yaml` from the `oas-deployment` repository is fetched automatically during the pipeline execution via sparse checkout.

3. **Pipeline Automation**:
   - The `Jenkinsfile` defines a robust pipeline for setting up the environment, running the script, and archiving the result as a downloadable artifact.

---

## Prerequisites
### To Run Locally:
- **Python 3.8+** installed.
- Install `pip` for managing dependencies.

### On Jenkins:
- Set up the following credentials in Jenkins:
  - **LOGZ_NA_TOKEN**: Token for accessing NA logs.
  - **LOGZ_EU_TOKEN**: Token for accessing EU logs.
- Ensure Jenkins has Git configured for repository access.
- Add Python to Jenkins' execution environment.

---

## Repository Structure
```plaintext
.
├── Jenkinsfile               # Jenkins pipeline definition
├── logz_metrics_handler.py   # Python script for querying logs
├── requirements.txt          # Python dependencies
├── README.md                 # Repository documentation (this file)
```

---

## Setup Instructions

### 1. Clone the Repository
```bash
git clone git@git.cias.one:tid/logzio-metrics-reporter.git
cd log-query-automation
```

### 2. Set Up Python Environment
Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate   # On Windows: venv\Scripts\activate
```

Install the dependencies:
```bash
pip install -r requirements.txt
```

### 3. Fetch `oas-deployment`’s `customers.yaml` File
Clone only the `customers.yaml` file using sparse checkout:
```bash
git init
git remote add origin git@git.cias.one:tid/oas-deployment
git sparse-checkout init
git sparse-checkout set customers.yaml
git pull origin main
```

---

## Usage Instructions

### Run the Script Locally
Execute the script with required arguments:
```bash
python logz_metrics_handler.py \
    --date 2025-01-26 \
    --start_time 08:00:00Z \
    --end_time 10:00:00Z \
    --time_range 14 \
    --eu_token "<EU_TOKEN>" \
    --na_token "<NA_TOKEN>" \
    --customers_file "customers.yaml"
```

### Jenkins Pipeline Execution
1. **Trigger the Pipeline**:
   - Open your Jenkins job linked to this repository.
   - Provide the following parameters:
     - **DATE**: Base date in the format `YYYY-MM-DD`.
     - **START_TIME**: Start time in the format `HH:mm:ssZ` (UTC).
     - **END_TIME**: End time in the format `HH:mm:ssZ` (UTC).
     - **TIME_RANGE**: Number of days before and after the base date.

2. **Pipeline Overview**:
   - The `Jenkinsfile` performs the following stages:
     - **Checkout Current Repository**: Fetches `logz_metrics_handler.py`.
     - **Checkout oas-deployment**: Uses sparse checkout to fetch `customers.yaml`.
     - **Setup Python Environment**: Creates and activates a virtual environment.
     - **Run logz_metrics_handler.py**: Executes the script with user-specified parameters.
     - **Archive Results**: Saves `output.csv` as a Jenkins build artifact.

3. **Download Artifacts**:
   - After a successful build, find `output.csv` in the **Artifacts** section on the Jenkins web interface.

---

## Example Pipeline Parameters

| Parameter    | Value        | Description                                    |
|--------------|--------------|------------------------------------------------|
| `DATE`       | `2025-01-26` | Base date for querying logs.                   |
| `START_TIME` | `08:00:00Z`  | Start time in UTC.                             |
| `END_TIME`   | `10:00:00Z`  | End time in UTC.                               |
| `TIME_RANGE` | `14`         | Number of days before and after the base date. |

---

## Output
At the end of the pipeline (or after running the script locally), an `output.csv` file is generated. This CSV contains:
- Log details based on the queried parameters.
- Data organized for further analysis and reporting.

For Jenkins jobs, the file is archived as a build artifact and can be downloaded directly from the Jenkins interface.

---

## Troubleshooting

### Common Errors:
1. **Error: Unable to Locate customers.yaml**:
   - Ensure `customers.yaml` is available under the specified `oas-deployment` directory.
   - Check the sparse checkout configuration in the pipeline or your local setup.

2. **Python Errors During Script Execution**:
   - Ensure all required dependencies are installed using `pip install -r requirements.txt`.
   - Activate the virtual environment before running the script.

3. **Jenkins Credential Errors**:
   - Verify that the tokens (`LOGZ_NA_TOKEN` and `LOGZ_EU_TOKEN`) are correctly configured in Jenkins credentials and accessible by the pipeline.

---

## Contributing
If you'd like to contribute:
1. Fork the repository.
2. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature
   ```
3. Commit your changes and push:
   ```bash
   git commit -m "Add your changes"
   git push origin feature/your-feature
   ```
4. Open a pull request!

---
