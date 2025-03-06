import argparse
import csv
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from multiprocessing import Pool
from pydoc import html
from typing import List, Tuple, Dict, Any, Union

import requests
import yaml
from atlassian import Confluence
from requests.exceptions import RequestException, HTTPError
from tabulate import tabulate

# Constants - Centralized for better readability and maintainability
EU_ENVIRONMENT = 'eu01-prd'
NA_ENVIRONMENT = 'na01-prd'
BASE_API_URL_EU = "https://api-eu.logz.io/v1/search"
BASE_API_URL_NA = "https://api.logz.io/v1/search"
NAMESPACE_PREFIX = 'tid-{platform_prefix}-'
NAMESPACE_SUFFIX = '-oas'
DEFAULT_CSV_FILENAME = "output.csv"

# Retry configuration for HTTP requests
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2

# ==========================
# Utility Functions
# ==========================

def get_headers(token: str) -> Dict[str, str]:
    """
    Generate HTTP headers for the API request.

    Args:
        token (str): The API token for the specified environment.

    Returns:
        Dict[str, str]: The HTTP headers dictionary.
    """
    return {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'X-API-TOKEN': token
    }


def get_url(environment: str) -> str:
    """
    Get the correct API URL based on the environment.

    Args:
        environment (str): Environment name ('eu01-prd' or 'na01-prd').

    Returns:
        str: Corresponding base URL for the API.
    """
    return BASE_API_URL_NA if environment == NA_ENVIRONMENT else BASE_API_URL_EU


def load_query_config(config_file: str) -> Dict[str, Any]:
    """
    Load queries from an external JSON configuration file.

    Args:
        config_file (str): Path to the configuration JSON file.

    Returns:
        Dict[str, Any]: The loaded query configurations.
    """
    try:
        # Load query configuration from a JSON file
        config_file_path = os.path.join(os.path.dirname(__file__), config_file)

        if not os.path.exists(config_file_path):
            raise FileNotFoundError(f"Configuration file not found at {config_file_path}")

        with open(config_file_path, "r") as file:
            query_config = json.load(file)

    except FileNotFoundError as fnf_error:
        raise FileNotFoundError(f"Error loading configuration file: {fnf_error}")
    except json.JSONDecodeError as jde_error:
        raise ValueError(f"Configuration file is not a valid JSON format: {jde_error}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error while loading query configuration: {str(e)}")
    return query_config


def replace_placeholders(query: Dict[str, Any], start_time: str, end_time: str, namespace: str = None,
                         tenant: str = None, response_code: str = None, platform: str = None) -> Dict[str, Any]:
    """
    Replace placeholders in the query template with actual values.

    Args:
        query (Dict[str, Any]): Query template with placeholders.
        start_time (str): Query start time (ISO8601 format).
        end_time (str): Query end time (ISO8601 format).
        namespace (str): Namespace to include in the query.
        tenant (str): tenant for the query.
        response_code (str): status code for the HTTP request.
        platform (str): platform for the query.

    Returns:
        Dict[str, Any]: The updated query with placeholders replaced.
    """
    query_str = json.dumps(query)  # Convert query dictionary to a string
    updated_query_str = query_str.replace("PLACEHOLDER_START_TIME", start_time)
    updated_query_str = updated_query_str.replace("PLACEHOLDER_END_TIME", end_time)
    if namespace:
        updated_query_str = updated_query_str.replace("PLACEHOLDER_NAMESPACE", f"{namespace}")
    if tenant:
        updated_query_str = updated_query_str.replace("PLACEHOLDER_TENANT", tenant)
    if response_code:
        updated_query_str = updated_query_str.replace("PLACEHOLDER_RESPONSE_CODE", response_code)
    if platform:
        updated_query_str = updated_query_str.replace("PLACEHOLDER_PLATFORM", platform)
    return json.loads(updated_query_str)  # Convert string back to a dictionary


def get_response_codes(section: str) -> List[str]:
    """
    Returns response codes for the given section in the JSON configuration file.

    Args:
        section (str): section from the configuration JSON file.

    Returns:
        List[str]: List of response codes.
    """
    # Extract response codes specific to the section
    section_config = query_config.get(section, {})
    response_codes = section_config.get("response_codes", [])
    if not response_codes:
        raise KeyError(f"Missing 'response_codes' in section: {section}")
    return response_codes


# ==========================
# Query config contents
# ==========================

query_config = load_query_config("queries_config.json")


# ==========================
# Core Utility Functions
# ==========================

def fetch_namespaces(customers_file: str, platform_prefix: str) -> Dict[str, List[str]]:
    """
    Fetch customer namespaces from a YAML file for each environment.

    Args:
        customers_file (str): Path to the customers.yaml file.
        platform_prefix (str): Prefix for filtering namespaces.

    Returns:
        Dict[str, List[str]]: A dictionary mapping environments to namespaces.
    """
    try:
        with open(customers_file, 'r') as file:
            data = yaml.safe_load(file)
    except (FileNotFoundError, yaml.YAMLError) as e:
        raise RuntimeError(f"Error loading customers YAML file: {e}")

    # Initialize namespace lists for each environment
    environments: Dict[str, List[str]] = {EU_ENVIRONMENT: [], NA_ENVIRONMENT: []}

    # Iterate over environments in YAML file, process namespaces
    for env, namespaces in environments.items():
        target_envs = data.get('platforms', {}).get(env, {}).get('targetEnvironments', [])
        for env_data in target_envs:
            namespace = env_data.get('namespace', '')
            if namespace.startswith(NAMESPACE_PREFIX.format(platform_prefix=platform_prefix)) and namespace.endswith(
                    NAMESPACE_SUFFIX):
                # Extract namespace without prefix and suffix
                namespaces.append(
                    namespace[len(NAMESPACE_PREFIX.format(platform_prefix=platform_prefix)):-len(NAMESPACE_SUFFIX)])

    return environments


def build_oca_queries(namespace_lists: Dict[str, List[str]], start_time: str, end_time: str, platform_prefix: str) \
        -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """
    Build search queries for all requests and failed requests for each namespace
    using external query configurations.

    Args:
        namespace_lists (Dict[str, List[str]]): Namespaces grouped by environment.
        start_time (str): Query start time (ISO8601 format).
        end_time (str): Query end time (ISO8601 format).
        platform_prefix (str): Platform prefix for namespaces.

    Returns:
        Dict[str, Dict[str, List[Dict[str, Any]]]]: Nested dictionary of queries (by environment and namespace).

    Raises:
        FileNotFoundError: If the configuration file is missing.
        KeyError: If required keys are missing in the JSON configuration.
        ValueError: If the query construction has mismatches or failures.
    """

    try:
        # Namespace prefix (used for formatting the namespace field)
        namespace_name = f"{NAMESPACE_PREFIX.format(platform_prefix=platform_prefix)}ws"

        # Validate query configuration
        if "oca_queries" not in query_config:
            raise KeyError("Missing 'oca_queries' section in query configuration")
        if "total_requests" not in query_config["oca_queries"]:
            raise KeyError("Missing 'total_requests' query in 'oca_queries'")
        if "failed_requests" not in query_config["oca_queries"]:
            raise KeyError("Missing 'failed_requests' query in 'oca_queries'")

        # Prepare the actual queries, grouped by environment and namespace
        results = {}
        for environment, namespaces in namespace_lists.items():
            if not isinstance(namespaces, list):
                raise ValueError(f"Namespaces for environment '{environment}' should be a list")

            results[environment] = {}
            for tenant in namespaces:
                try:
                    # Total requests query
                    total_requests_query = replace_placeholders(
                        query_config["oca_queries"]["total_requests"],
                        start_time=start_time,
                        end_time=end_time,
                        namespace=namespace_name,
                        tenant=tenant
                    )

                    # Failed requests query
                    failed_requests_query = replace_placeholders(
                        query_config["oca_queries"]["failed_requests"],
                        start_time=start_time,
                        end_time=end_time,
                        namespace=namespace_name,
                        tenant=tenant
                    )

                    results[environment][tenant] = [total_requests_query, failed_requests_query]

                except KeyError as ke:
                    raise KeyError(
                        f"Failed to replace placeholders for tenant '{tenant}' in environment '{environment}': {ke}")
                except Exception as e:
                    raise ValueError(
                        f"Error occurred while building query for tenant '{tenant}' in environment '{environment}': {str(e)}")

        return results

    except KeyError as ke:
        raise KeyError(f"Key error encountered in query configuration or namespace lists: {ke}")
    except ValueError as ve:
        raise ValueError(f"Invalid input or query construction error: {ve}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error while building OCA queries: {str(e)}")


def build_request_queries(platform_prefix: str, start_time: str, end_time: str, section: str) -> Dict[str, Any]:
    """
    Build queries for request distribution with specific time ranges and response codes
    using external query configurations.

    Args:
        platform_prefix (str): Platform ("prd" or "stg").
        start_time (str): Start time in HH:mm:ssZ format.
        end_time (str): End time in HH:mm:ssZ format.
        section (str): Section of the query configuration to use ("request_distribution_query" or "soap_requests_query").

    Returns:
        Dict[str, Any]: Queries for response codes and total requests.

    Raises:
        FileNotFoundError: If the configuration file is missing.
        KeyError: If required keys are missing in the JSON configuration.
        ValueError: If a query cannot be successfully built.
    """

    try:
        # Namespace name (formatted from platform prefix)
        namespace_name = f"tid-{platform_prefix}-ws"

        response_codes = get_response_codes(section)

        # Prepare queries for each response code
        queries = {}
        for response_code in response_codes + ["total"]:
            base_query = query_config[section]  # Load the base query template

            # Get the additional logic for response code conditions
            if response_code == "total":
                additional_condition = \
                    query_config[section]["additional_logic"]["response_code_condition"]["total"]
            else:
                additional_condition = \
                    query_config[section]["additional_logic"]["response_code_condition"]["specific"]

            # Safely extract "bool" and "must" from the base query
            base_bool = base_query.get("query", {}).get("bool", {})
            if not base_bool:
                raise KeyError(f"Missing 'bool' in base query for response code: {response_code}")

            base_must = base_bool.get("must", [])
            if not isinstance(base_must, list):
                raise ValueError(f"Invalid format for 'must' in base query for response code: {response_code}")

            # Combine the "must" condition with the additional logic
            base_query_with_condition = {
                "query": {
                    "bool": {
                        "must": base_must + [additional_condition]  # Merge "must" conditions
                    }
                }
            }

            # Attempt to replace placeholders and validate the query
            try:
                queries[response_code] = replace_placeholders(
                    base_query_with_condition,
                    start_time=start_time,
                    end_time=end_time,
                    namespace=namespace_name,
                    response_code=response_code,
                    platform=platform_prefix
                )
            except Exception as replace_error:
                raise ValueError(f"Error replacing placeholders for response code {response_code}: {replace_error}")

        return queries

    except KeyError as ke:
        raise KeyError(f"Missing required keys in query configuration: {ke}")
    except ValueError as ve:
        raise ValueError(f"Validation error occurred while building queries: {ve}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error occurred while building queries: {str(e)}")


def query_osra_errors(
        environment: str,
        start_time: str,
        end_time: str,
        eu_token: str,
        na_token: str
) -> Tuple[int, List[Dict[str, str]]]:
    """
    Query OSRA DataCollector errors from Logz.io for the given time range.

    Args:
        environment (str): Target environment (e.g., 'eu01-prd' or 'na01-prd').
        start_time (str): The start time of the query in ISO8601 format.
        end_time (str): The end time of the query in ISO8601 format.
        eu_token (str): API token for the EU region.
        na_token (str): API token for the NA region.

    Returns:
        Tuple[int, List[Dict[str, str]]]: Total number of errors and a list of error details (message and tenant).

    Raises:
        FileNotFoundError: If the query configuration file does not exist.
        KeyError: If required keys are missing from the query configuration.
        ValueError: If there is an error during query construction.
        RuntimeError: If the query execution fails.
    """
    try:
        # Validate the existence of "osra_error_query" in the query configuration
        if "osra_error_query" not in query_config:
            raise KeyError("Missing 'osra_error_query' in query configuration")

        # Replace placeholders in the query
        osra_error_query = replace_placeholders(
            query_config["osra_error_query"],
            start_time=start_time,
            end_time=end_time,
            namespace=""  # No namespace is needed here
        )

    except KeyError as ke:
        raise KeyError(f"Key error during placeholder replacement: {ke}")
    except ValueError as ve:
        raise ValueError(f"Error replacing placeholders in 'osra_error_query': {ve}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error while constructing OSRA error query: {str(e)}")

    try:
        # Execute the query to fetch hits and error details
        total_errors, error_details = execute_query(
            environment=environment,
            query=osra_error_query,
            eu_token=eu_token,
            na_token=na_token,
            return_details=True
        )

        return total_errors, error_details

    except Exception as e:
        raise RuntimeError(f"Query execution failed for environment '{environment}': {str(e)}")


def fetch_request_distribution(
        platform_prefix: str,
        start_time: str,
        end_time: str,
        eu_token: str,
        na_token: str,
        section: str
) -> Dict[str, Dict[str, int]]:
    """
    Fetch request counts for a specific date and time range from both EU and NA environments.

    Args:
        platform_prefix (str): Platform prefix for the namespace (e.g., "prd" or "stg").
        start_time (str): Start time in HH:mm:ssZ format.
        end_time (str): End time in HH:mm:ssZ format.
        eu_token (str): API token for EU cluster.
        na_token (str): API token for NA cluster.
        section (str): Section of the query configuration to use ("request_distribution_query" or "soap_requests_query").

    Returns:
        Dict[str, Dict[str, int]]: Results grouped by response code, with counts for both EU and NA environments.
    """
    # Prepare queries
    queries = build_request_queries(platform_prefix, start_time, end_time, section)

    # Initialize results in the desired format
    response_codes = get_response_codes(section)
    results = {code: {EU_ENVIRONMENT: 0, NA_ENVIRONMENT: 0} for code in response_codes + ["total"]}

    # Helper function to execute a query
    def execute_task(environment, code, query, token):
        return code, environment, execute_query(environment, query, token, na_token)

    # Use ThreadPoolExecutor for parallel execution
    with ThreadPoolExecutor() as executor:
        # Create futures for EU and NA queries
        futures = [
                      executor.submit(execute_task, EU_ENVIRONMENT, code, query, eu_token)
                      for code, query in queries.items()
                  ] + [
                      executor.submit(execute_task, NA_ENVIRONMENT, code, query, na_token)
                      for code, query in queries.items()
                  ]

        # Fetch results as tasks complete
        for future in futures:
            code, environment, result = future.result()
            results[code][environment] = result

    return results


def generate_requests_distribution_table(
        platform_prefix: str,
        date: str,
        start_time: str,
        end_time: str,
        eu_token: str,
        na_token: str,
        section: str,
        range_weeks: int = 1
) -> str:
    """
    Optimized: Generate the request distribution table with colspan header format for EU and NA data.

    Args:
        platform_prefix (str): Platform prefix for the namespace (e.g., "prd" or "stg").
        date (str): Date in YYYY-MM-DD format.
        start_time (str): Query start time.
        end_time (str): Query end time.
        eu_token (str): API token for EU cluster.
        na_token (str): API token for NA cluster.
        section (str): Section of the query configuration to use ("request_distribution_query" or "soap_requests_query").
        range_weeks (int): Number of weeks before and after the given date to include in the table.

    Returns:
        str: The request distribution table in HTML format with colspan headers.
    """
    from datetime import datetime, timedelta

    # Compute the list of dates dynamically
    given_date = datetime.strptime(date, "%Y-%m-%d")
    dates = [
        (given_date + timedelta(weeks=i)).strftime("%Y-%m-%d")
        for i in range(-range_weeks, range_weeks + 1)
    ]

    # Variables to track headers and totals (Combined computation for optimization)
    all_results = {}
    date_headers = ""
    time_headers = ""
    platform_headers = ""
    total_requests = {EU_ENVIRONMENT: [0] * len(dates), NA_ENVIRONMENT: [0] * len(dates)}  # Totals for EU and NA
    response_codes = get_response_codes(section)

    # Fetch data and build headers in a single loop
    for index, query_date in enumerate(dates):
        # Fetch data for this date
        results = fetch_request_distribution(
            platform_prefix,
            f"{query_date}T{start_time}",
            f"{query_date}T{end_time}",
            eu_token,
            na_token,
            section
        )
        all_results[query_date] = results

        # Build Date + Time headers as we iterate
        date_headers += f"<th colspan='2'>{query_date}</th>"
        time_headers += f"<th colspan='2'>{start_time}-{end_time}</th>"
        platform_headers += f"<th>EU-{platform_prefix.upper()}</th><th>NA-{platform_prefix.upper()}</th>"

        # Update totals for EU and NA for use in the totals row
        for code in response_codes:
            total_requests[EU_ENVIRONMENT][index] += results.get(code, {}).get(EU_ENVIRONMENT, 0)
            total_requests[NA_ENVIRONMENT][index] += results.get(code, {}).get(NA_ENVIRONMENT, 0)

    # Begin table construction
    html_table = "<table>"

    # Add the date header row
    html_table += f"<tr><th>Date</th>{date_headers}</tr>"

    # Add the time header row
    html_table += f"<tr><th>Time</th>{time_headers}</tr>"

    # Add the platform header row
    html_table += f"<tr><th>Return code &darr; Platform &rarr;</th>{platform_headers}</tr>"

    # Add rows for response codes
    for code in response_codes:
        row = f"<tr><td>{code}</td>"
        for query_date in dates:
            results = all_results[query_date]
            eu_count = results.get(code, {}).get(EU_ENVIRONMENT, 0)
            na_count = results.get(code, {}).get(NA_ENVIRONMENT, 0)
            row += f"<td>{eu_count}</td><td>{na_count}</td>"
        row += "</tr>"
        html_table += row

    # Add the total requests row
    html_table += f"<tr><td><b>Total Requests</b></td>"
    for i in range(len(dates)):
        eu_total = total_requests[EU_ENVIRONMENT][i]
        na_total = total_requests[NA_ENVIRONMENT][i]
        html_table += f"<td><b>{eu_total}</b></td><td><b>{na_total}</b></td>"
    html_table += "</tr>"

    # End table
    html_table += "</table>"

    return html_table


def execute_query(environment: str, query: Dict[str, Any], eu_token: str, na_token: str,
                  return_details: bool = False) -> Union[int, Tuple[int, List[Dict[str, str]]]]:
    """
    Execute a specific query against the Logz.io API with retry logic.

    Args:
        environment (str): Target environment ('eu01-prd' or 'na01-prd').
        query (Dict[str, Any]): Query payload for the API.
        eu_token (str): Logz.io API token for the EU region.
        na_token (str): Logz.io API token for the NA region.
        return_details (bool): If True, also return details of hits (e.g., error messages and tenants).

    Returns:
        Union[int, Tuple[int, List[Dict[str, str]]]]: Either the total number of hits (if `return_details` is False) or a tuple
        containing the total hits and a list of error details (if `return_details` is True).
    """
    url = get_url(environment)
    headers = get_headers(eu_token if environment != NA_ENVIRONMENT else na_token)

    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.post(url, headers=headers, json=query, timeout=15)
            response.raise_for_status()  # Raise HTTPError for bad responses

            # Parse response data
            response_data = response.json()
            hits = response_data.get("hits", {})
            total_hits = hits.get("total", 0)

            if return_details:
                error_details = [
                    {
                        "message": hit["_source"].get("message", ""),
                        "tenant": hit["_source"].get("tenant", "")
                    }
                    for hit in hits.get("hits", [])
                ]
                return total_hits, error_details

            return total_hits

        except (RequestException, HTTPError) as e:
            print(f"[ERROR] Query failed in {environment} (Retry {retries + 1}/{MAX_RETRIES}): {e}")
            retries += 1
            if retries == MAX_RETRIES:
                print("[ERROR] Maximum retries reached.")
                break
            print(f"[INFO] Retrying in {RETRY_BACKOFF_SECONDS} seconds...")
            time.sleep(RETRY_BACKOFF_SECONDS)

    # Return default values based on `return_details`
    return (0, []) if return_details else 0


def process_date_task(
        date: str,
        namespace_lists: Dict[str, List[str]],
        start_time: str,
        end_time: str,
        eu_token: str,
        na_token: str,
        platform_prefix: str
) -> Tuple[str, Dict[str, Dict[str, Tuple[int, int]]]]:
    """
    Process all queries for a specific date across all environments.

    Args:
        date (str): Date to process metrics for.
        namespace_lists (Dict[str, List[str]]): Namespaces categorized by environment.
        start_time (str): Start time of the query window.
        end_time (str): End time of the query window.
        eu_token (str): API token for EU region.
        na_token (str): API token for NA region.
        platform_prefix (str): Platform's prefix for filtering data.

    Returns:
        Tuple[str, Dict]: Date and environment-wise results (total/failed requests).
    """
    print(f"[INFO] Processing tasks for date: {date}")

    queries = build_oca_queries(namespace_lists, start_time, end_time, platform_prefix)
    date_results: Dict[str, Dict[str, Tuple[int, int]]] = {}

    for environment, query_list in queries.items():
        results = {}
        for namespace, query_set in query_list.items():
            # Ensure query set has exactly two queries (total and failed requests)
            if len(query_set) != 2:
                print(f"[WARNING] Missing queries for namespace {namespace}. Skipping...")
                continue

            # Execute total and failed queries
            total_requests = execute_query(environment, query_set[0], eu_token, na_token)
            failed_requests = execute_query(environment, query_set[1], eu_token, na_token)

            print(f"[INFO] Tenant: {namespace} on {date} during {start_time.split('T')[1]} - "
                  f"{end_time.split('T')[1]}, Total: {total_requests}, Failed: {failed_requests}.")
            results[namespace] = (total_requests, failed_requests)

        date_results[environment] = results

    return date, date_results


def log_results_and_export_to_csv(
        results: Dict[str, Dict[str, Dict[str, Tuple[int, int]]]],
        date_ranges: List[Tuple[str, str, str]],
        csv_filename: str = DEFAULT_CSV_FILENAME
) -> None:
    """
    Logs query results for each date and environment and saves them to a CSV file.

    Args:
        results (Dict[str, Dict[str, Dict[str, Tuple[int, int]]]]): Query results categorized by date and environment.
        date_ranges (List[Tuple[str, str, str]]): List of tuples with date, start, and end times.
        csv_filename (str): Output CSV file name.
    """
    with open(csv_filename, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        for date, environments in results.items():
            # Match date with respective start and end times
            start_time, end_time = next(
                ((start, end) for d, start, end in date_ranges if d == date), (None, None))

            print(
                f"\n========== Results for date: {date} ({start_time.split('T')[1]} {end_time.split('T')[1]}) ==========\n")
            csv_writer.writerow([f"Results for date: {date} ({start_time.split('T')[1]} {end_time.split('T')[1]})"])

            for environment, env_results in environments.items():
                print(f"--- Results for environment: {environment} ---\n")
                csv_writer.writerow([f"# Results for environment: {environment}"])

                headers = ["Customer", "Total Requests", "Failed Requests", "% Failure"]
                table: List[List[Any]] = []
                total_requests = 0
                total_failed_requests = 0

                for customer, (total_requests_customer, failed_requests_customer) in env_results.items():
                    failure_percentage = (
                        (failed_requests_customer / total_requests_customer) * 100
                        if total_requests_customer > 0
                        else 0
                    )
                    table.append(
                        [customer, total_requests_customer, failed_requests_customer, f"{failure_percentage:.2f}%"]
                    )
                    total_requests += total_requests_customer
                    total_failed_requests += failed_requests_customer

                total_failure_percentage = (
                    (total_failed_requests / total_requests) * 100 if total_requests > 0 else 0
                )
                table.append(["Total", total_requests, total_failed_requests, f"{total_failure_percentage:.2f}%"])

                print(tabulate(table, headers=headers, tablefmt="pretty"))
                print()

                csv_writer.writerow(headers)
                csv_writer.writerows(table)
                csv_writer.writerow([])  # Blank line between sections


def create_confluence_page(
        confluence_url: str, username: str, password: str,
        space_key: str, page_title: str,
        findings: Dict[str, Dict[str, Dict[str, Tuple[int, int]]]],
        osra_errors: Dict[str, Tuple[int, List[Dict[str, str]]]],
        all_requests_results: str,
        soap_requests_results: str,
        date_ranges: List[Tuple[str, str, str]],
        invoker_info: str
) -> None:
    """
    Creates a Confluence page with detailed findings and metrics.

    Args:
        confluence_url (str): Base URL of the Confluence server.
        username (str): Username to authenticate with Confluence.
        password (str): API token or password for authentication.
        space_key (str): Confluence space key.
        page_title (str): Title of the new Confluence page.
        findings (Dict): Results grouped by date and environment.
        osra_errors (Dict): OSRA DataCollector errors by date.
        all_requests_results (str): All requests table in HTML format.
        soap_requests_results (str): Soap requests table in HTML format.
        date_ranges (List[Tuple[str, str, str]]): List of tuples with date, start, and end times.
        invoker_info (str): Invoker of the script.
    """
    try:
        confluence = Confluence(url=confluence_url, username=username, password=password)

        content = f"<h3>This report generated by <a href=\"{invoker_info}\">{invoker_info}</a>.</h3>"

        for date, environments in findings.items():
            start_time, end_time = next(
                ((start, end) for d, start, end in date_ranges if d == date), (None, None)
            )

            content += f"<h4>Results for {date} ({start_time.split('T')[1]} to {end_time.split('T')[1]})</h4>"

            # Side-by-side display for environments
            content += "<table style='width: 100%; table-layout: fixed;'><thead><tr>"

            for environment in environments:
                content += f"<th style='width: 50%; text-align: center;'>{environment}</th>"

            content += "</tr></thead><tbody><tr>"

            for environment, env_results in environments.items():
                content += "<td style='vertical-align: top;'>"
                total_requests = 0
                total_failed_requests = 0

                content += "<table style='width: 100%; border: 1px solid black;'>"
                content += "<thead><tr><th>Customer</th><th>Total Requests</th><th>Failed Requests</th><th>% Failure</th></tr></thead><tbody>"
                for customer, (req, fail) in env_results.items():
                    failure_percentage = (fail / req) * 100 if req > 0 else 0
                    content += f"<tr><td>{customer}</td><td>{req}</td><td>{fail}</td><td>{failure_percentage:.2f}%</td></tr>"
                    total_requests += req
                    total_failed_requests += fail
                total_failure_percentage = (
                    (total_failed_requests / total_requests) * 100 if total_requests > 0 else 0
                )
                content += f"<tr><td><b>Total</b></td><td><b>{total_requests}</b></td><td><b>{total_failed_requests}</b></td><td><b>{total_failure_percentage:.2f}%</b></td></tr>"

                content += "</tbody></table></td>"
            content += "</tr></tbody></table>"

        # Add OSRA DataCollector errors
        content += "<h2>OSRA DataCollector</h2>"
        for date, (total, error_details) in osra_errors.items():
            content += f"<h3>Date: {date}</h3>"
            content += f"<p>Total Errors: {total}</p>"
            # Only create the table if there are errors
            if total > 0:
                content += "<table style='width: 100%; border: 1px solid black;'>"
                content += "<thead><tr><th>Tenant</th><th>Message</th></tr></thead><tbody>"

                for item in error_details:
                    tenant = item['tenant']
                    # Truncate the message to 200 characters
                    message = item['message'][:500] + '...' if len(item['message']) > 200 else item['message']
                    message = message.replace("\n", " ").replace("\r", "")
                    message = html.escape(message)  # Strip out any line breaks
                    content += f"<tr><td>{tenant}</td><td>{message}</td></tr>"

                content += "</tbody></table>"

        content += "<h2>All Requests Distribution</h2>"
        content += f"<p>{all_requests_results}</p>"

        content += "<h2>SOAP Requests Distribution</h2>"
        content += f"<p>{soap_requests_results}</p>"

        existing_page = confluence.get_page_by_title(space=space_key, title=page_title)

        if existing_page:
            page_id = existing_page['id']
            response = confluence.update_page(page_id=page_id, title=page_title, body=content)
            page_url = response.get('_links', {}).get('base') + response.get('_links', {}).get('webui', '')
            print(f"[INFO] Confluence page updated successfully! URL: {page_url}")
        else:
            response = confluence.create_page(space=space_key, title=page_title, body=content)
            page_url = response.get('_links', {}).get('base') + response.get('_links', {}).get('webui', '')
            print(f"[INFO] Confluence page created successfully! URL: {page_url}")
    except RequestException as e:
        print(f"[ERROR] Confluence connection issue: {e}")


def generate_date_ranges(base_date: str, date_offset_range: int, start_time: str, end_time: str) -> List[
    Tuple[str, str, str]]:
    """
    Generate a range of dates and their query periods.

    Args:
        base_date (str): The base date in YYYY-MM-DD format.
        date_offset_range (int): Number of days before and after the base date.
        start_time (str): Start time of each day (HH:mm:ssZ format).
        end_time (str): End time of each day (HH:mm:ssZ format).

    Returns:
        List[Tuple[str, str, str]]: List containing date and start/end times.
    """
    base_date_obj = datetime.strptime(base_date, "%Y-%m-%d")
    start_window = base_date_obj - timedelta(days=date_offset_range)
    end_window = base_date_obj + timedelta(days=date_offset_range)

    return [
        (
            (start_window + timedelta(days=i)).strftime("%Y-%m-%d"),
            f"{(start_window + timedelta(days=i)).strftime('%Y-%m-%d')}T{start_time}",
            f"{(start_window + timedelta(days=i)).strftime('%Y-%m-%d')}T{end_time}"
        )
        for i in range((end_window - start_window).days + 1)
    ]


if __name__ == "__main__":
    # Define argument parser
    parser = argparse.ArgumentParser(description="Process log queries and save results to CSV/Confluence.")
    parser.add_argument('--platform', required=True, help="Platform (prd or stg).")
    parser.add_argument('--date', required=True, help="Base date (YYYY-MM-DD).")
    parser.add_argument('--start_time', required=True, help="Start time (HH:mm:ssZ).")
    parser.add_argument('--end_time', required=True, help="End time (HH:mm:ssZ).")
    parser.add_argument('--date_offset_range', type=int, required=True, help="Number of days before/after base date.")
    parser.add_argument('--eu_token', required=True, help="Logz.io EU API token.")
    parser.add_argument('--na_token', required=True, help="Logz.io NA API token.")
    parser.add_argument('--customers_file', required=True, help="Path to customers.yaml.")
    parser.add_argument('--csv_filename', default=DEFAULT_CSV_FILENAME, help="Output CSV filename.")
    parser.add_argument('--confluence_url', default="https://jira.onespan.com/confluence", help="Confluence base URL.")
    parser.add_argument('--confluence_username', required=True, help="Confluence username (or email).")
    parser.add_argument('--confluence_password', required=True, help="Confluence password.")
    parser.add_argument('--space_key', default="TeamSystemEngineering", help="Confluence space key.")
    parser.add_argument('--page_title', required=True, help="Title for the Confluence page.")
    parser.add_argument('--invoker_info', default="logz_metrics_handler.py", help="Invoker of this script (docker container, jenkins job etc).")

    # Parse arguments
    args = parser.parse_args()

    try:
        # Fetch namespaces
        namespace_lists = fetch_namespaces(args.customers_file, args.platform)

        # Generate date ranges
        date_ranges = generate_date_ranges(args.date, args.date_offset_range, args.start_time, args.end_time)

        # Process all dates using multiprocessing
        print("[INFO] Starting processing for OCA queries...")
        all_date_results: Dict[str, Dict[str, Dict[str, Tuple[int, int]]]] = {}

        with Pool() as pool:
            tasks = [
                (date, namespace_lists, start_time, end_time, args.eu_token, args.na_token, args.platform)
                for date, start_time, end_time in date_ranges
            ]

            for date, date_results in pool.starmap(process_date_task, tasks):
                all_date_results[date] = date_results

        print("[INFO] Processing completed for OCA queries.")

        print("[INFO] Starting processing for OSRA queries...")
        osra_total_errors, osra_error_details = query_osra_errors(
            environment=args.platform,
            start_time=f"{args.date}T{args.start_time}",
            end_time=f"{args.date}T{args.end_time}",
            eu_token=args.eu_token,
            na_token=args.na_token
        )

        osra_results = {args.date: (osra_total_errors, osra_error_details)}
        print("[INFO] Processing completed for OSRA queries...")

        print("[INFO] Starting processing for requests distribution queries...")
        # Generate HTML table
        all_requests_table = generate_requests_distribution_table(args.platform, args.date, args.start_time,
                                                                  args.end_time, args.eu_token, args.na_token,
                                                                  section="request_distribution_query")
        print("[INFO] Processing completed for requests distribution queries...")

        print("[INFO] Starting processing for SOAP requests queries...")
        # Generate HTML table
        soap_requests_table = generate_requests_distribution_table(args.platform, args.date, args.start_time,
                                                                   args.end_time, args.eu_token, args.na_token,
                                                                   section="soap_requests_query")
        print("[INFO] Processing completed for SOAP requests queries...")

        # Save results to CSV
        log_results_and_export_to_csv(all_date_results, date_ranges, args.csv_filename)

        # Upload results to Confluence
        print("[INFO] Uploading results to Confluence...")
        create_confluence_page(
            confluence_url=args.confluence_url,
            username=args.confluence_username,
            password=args.confluence_password,
            space_key=args.space_key,
            page_title=args.page_title,
            findings=all_date_results,
            osra_errors=osra_results,
            all_requests_results=all_requests_table,
            soap_requests_results=soap_requests_table,
            date_ranges=date_ranges,
            invoker_info=args.invoker_info
        )

    except Exception as e:
        print(f"[ERROR] An error occurred: {e}")
