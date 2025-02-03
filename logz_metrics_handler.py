import argparse
import csv
from datetime import datetime, timedelta
from multiprocessing import Pool
from typing import List, Tuple, Dict, Any

import requests
import yaml
from tabulate import tabulate
from atlassian import Confluence  # Used for the Confluence Integration


# Utility Functions
def get_headers(token: str) -> Dict[str, str]:
    """
    Generate HTTP headers for the API request.

    Args:
        token (str): The API token either for EU or NA environments.

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
        environment (str): The environment name ('eu01-prd' or 'na01-prd').

    Returns:
        str: The appropriate base URL for the API.
    """
    return "https://api.logz.io/v1/search" if environment == 'na01-prd' else "https://api-eu.logz.io/v1/search"


# Core Functions
def log_results_and_export_to_csv(
        results: Dict[str, Dict[str, Dict[str, Tuple[int, int]]]],
        date_ranges: List[Tuple[str, str, str]],
        csv_filename: str = "output.csv"
) -> None:
    """
    Logs query results for each date and environment and saves them to a CSV file.

    Args:
        results (Dict[str, Dict[str, Dict[str, Tuple[int, int]]]]): Query results categorized by date and environment.
        date_ranges (List[Tuple[str, str, str]]): List of tuples with date, start, and end times.
        csv_filename (str): Output CSV file name (default: "output.csv").
    """
    with open(csv_filename, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        for date, environments in results.items():
            # Match date with its respective start and end times
            start_time, end_time = next(
                ((start, end) for d, start, end in date_ranges if d == date), (None, None))

            print(f"\n========== Results for date: {date} (Time: {start_time} to {end_time}) ==========\n")
            csv_writer.writerow([f"# Results for date: {date} (Time: {start_time} to {end_time})"])

            for environment, env_results in environments.items():
                print(f"--- Results for environment: {environment} ---\n")
                csv_writer.writerow([f"# Results for environment: {environment}"])

                # Define table headers
                headers = ["Customer", "Total Requests", "Failed Requests", "% Failure"]
                table: List[List[Any]] = []
                total_requests = 0
                total_failed_requests = 0

                # Populate the table
                for customer, (total_requests_customer, failed_requests_customer) in env_results.items():
                    failure_percentage = (
                        (failed_requests_customer / total_requests_customer) * 100
                        if total_requests_customer > 0
                        else 0
                    )
                    table.append([
                        customer, total_requests_customer, failed_requests_customer, f"{failure_percentage:.2f}%"
                    ])
                    # Aggregate totals
                    total_requests += total_requests_customer
                    total_failed_requests += failed_requests_customer

                # Add a final row showing the overall totals
                total_failure_percentage = (
                    (total_failed_requests / total_requests) * 100 if total_requests > 0 else 0
                )
                table.append(["Total", total_requests, total_failed_requests, f"{total_failure_percentage:.2f}%"])

                print(tabulate(table, headers=headers, tablefmt="pretty"))
                print()

                # Write data to CSV
                csv_writer.writerow(headers)
                csv_writer.writerows(table)
                csv_writer.writerow([])  # Blank line between sections


def fetch_namespaces(customers_file: str) -> Dict[str, List[str]]:
    """
    Fetch customer namespaces from a YAML file for each environment.

    Args:
        customers_file (str): Path to the customers.yaml file.

    Returns:
        Dict[str, List[str]]: A dictionary with namespaces categorized by environment.
    """
    with open(customers_file, 'r') as file:
        data = yaml.safe_load(file)

    environments: Dict[str, List[str]] = {'eu01-prd': [], 'na01-prd': []}
    for env, namespaces in environments.items():
        # Parse target environments and filter valid namespaces
        target_envs = data.get('platforms', {}).get(env, {}).get('targetEnvironments', [])
        for env in target_envs:
            namespace = env.get('namespace', '')
            if namespace.startswith('tid-prd-') and namespace.endswith('-oas'):
                namespaces.append(namespace[len('tid-prd-'):-len('-oas')])
    return environments


def build_queries(namespace_lists: Dict[str, List[str]], start_time: str, end_time: str) -> Dict[
    str, Dict[str, List[Dict[str, Any]]]]:
    """
    Build search queries for all requests and failed requests for each namespace.

    Args:
        namespace_lists (Dict[str, List[str]]): The namespace lists categorized by environment.
        start_time (str): The start time of the query (ISO8601 format).
        end_time (str): The end time of the query (ISO8601 format).

    Returns:
        Dict[str, Dict[str, List[Dict[str, Any]]]]: A nested dictionary of queries grouped by environment and namespace.
    """
    return {
        environment: {
            namespace: [
                # Query for total requests
                {
                    "query": {
                        "bool": {
                            "must": [
                                {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}},
                                {"term": {"kubernetes.namespace_name": "tid-prd-ws"}},
                                {"exists": {"field": "upstream_status"}},
                                {"term": {"tenant": namespace}}
                            ]
                        }
                    },
                    "size": 0
                },
                # Query for failed requests
                {
                    "query": {
                        "bool": {
                            "must": [
                                {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}},
                                {"term": {"kubernetes.namespace_name": "tid-prd-ws"}},
                                {"exists": {"field": "upstream_status"}},
                                {"term": {"tenant": namespace}},
                                {"term": {"upstream_status": "500"}}
                            ]
                        }
                    },
                    "size": 0
                }
            ]
            for namespace in namespaces
        }
        for environment, namespaces in namespace_lists.items()
    }


def execute_query(environment: str, query: Dict[str, Any], eu_token: str, na_token: str) -> int:
    """
    Execute a given query against the API.

    Args:
        environment (str): The environment name ('eu01-prd' or 'na01-prd').
        query (Dict[str, Any]): The query payload.
        eu_token (str): The API token for EU environment.
        na_token (str): The API token for NA environment.

    Returns:
        int: The total number of matching hits for the query.
    """
    url = get_url(environment)
    headers = get_headers(eu_token if environment != 'na01-prd' else na_token)

    response = requests.post(url, headers=headers, json=query)

    if response.status_code != 200:
        print(f"[ERROR] Query failed in {environment}: {response.status_code} - {response.text}")
        return 0

    return response.json().get("hits", {}).get("total", 0)


def process_date_task(
        date: str,
        namespace_lists: Dict[str, List[str]],
        start_time: str,
        end_time: str,
        eu_token: str,
        na_token: str
) -> Tuple[str, Dict[str, Dict[str, Tuple[int, int]]]]:
    """
    Process queries for a specific date in all environments.

    Args:
        date (str): The date to process.
        namespace_lists (Dict[str, List[str]]): Namespaces grouped by environment.
        start_time (str): Start time of the query period.
        end_time (str): End time of the query period.
        eu_token (str): Logz.io API token for EU environment.
        na_token (str): Logz.io API token for NA environment.

    Returns:
        Tuple[str, Dict[str, Dict[str, Tuple[int, int]]]]: The date and corresponding results.
    """
    print(f"[INFO] Starting processing for date: {date}")

    # Build queries for this date
    queries = build_queries(namespace_lists, start_time, end_time)
    date_results: Dict[str, Dict[str, Tuple[int, int]]] = {}

    for environment, query_list in queries.items():
        print(f"[INFO] Processing environment: {environment} for date: {date}")
        results: Dict[str, Tuple[int, int]] = {}

        for namespace, query_set in query_list.items():
            if len(query_set) != 2:
                print(f"[WARNING] Missing queries for namespace: {namespace}. Skipping...")
                continue

            # Execute queries
            total_requests = execute_query(environment, query_set[0], eu_token, na_token)
            failed_requests = execute_query(environment, query_set[1], eu_token, na_token)

            print(f"[INFO] Namespace: {namespace}, Total: {total_requests}, Failed: {failed_requests}.")
            results[namespace] = (total_requests, failed_requests)

        date_results[environment] = results

    print(f"[INFO] Completed processing for date: {date}")
    return date, date_results


def create_confluence_page(confluence_url: str, username: str, api_token: str, space_key: str, page_title: str,
                           findings: Dict[str, Dict[str, Dict[str, Tuple[int, int]]]]) -> None:
    """
    Creates a Confluence page with detailed findings and metrics,
    displaying environments side by side for the given dates.

    Args:
        confluence_url: The base URL of the Confluence server where the page
            should be created.
        username: The username required to authenticate against the Confluence
            server.
        api_token: The API token or password for authenticating the user.
        space_key: The Confluence space key where the page will be created.
        page_title: The title of the new Confluence page.
        findings: A dictionary mapping dates to a hierarchy of environments and
            their customer metrics. Each metric includes the customer name,
            total requests, failed requests, and a computed failure percentage.
    """
    confluence = Confluence(
        url=confluence_url,
        username=username,
        password=api_token
    )

    content = f"<h1>{page_title}</h1>"

    for date, environments in findings.items():
        # Start date section
        content += f"<h2>Results for {date}</h2>"

        # Create the side-by-side display table (2 columns for environments)
        content += "<table style='width: 100%; table-layout: fixed;'><thead><tr>"

        # Add environment column headers
        for environment in environments:
            content += f"<th style='width: 50%; text-align: center;'>{environment}</th>"

        content += "</tr></thead><tbody><tr>"

        # Add tables side by side for each environment
        for environment, env_results in environments.items():
            content += "<td style='vertical-align: top;'>"

            # Initialize totals for this environment
            total_requests = 0
            total_failed_requests = 0

            # Create the individual environment table
            content += "<table style='width: 100%; border: 1px solid #ddd; border-collapse: collapse;'>"
            content += "<thead style='background-color: #f0f0f0;'><tr>"
            content += "<th>Customer</th><th>Total Requests</th><th>Failed Requests</th><th>% Failure</th>"
            content += "</tr></thead><tbody>"

            # Add rows for each customer in this environment
            for customer, (requests, failed) in env_results.items():
                # Update totals
                total_requests += requests
                total_failed_requests += failed

                # Compute failure percentage
                failure_percentage = (failed / requests) * 100 if requests > 0 else 0
                content += f"<tr><td>{customer}</td><td>{requests}</td><td>{failed}</td><td>{failure_percentage:.2f}%</td></tr>"

            # Add total row for this environment
            total_failure_percentage = (total_failed_requests / total_requests) * 100 if total_requests > 0 else 0
            content += f"<tr><td><b>Total</b></td><td><b>{total_requests}</b></td><td><b>{total_failed_requests}</b></td><td><b>{total_failure_percentage:.2f}%</b></td></tr>"

            content += "</tbody></table></td>"

        content += "</tr></tbody></table>"

    # Check if the page already exists
    existing_page = confluence.get_page_by_title(space=space_key, title=page_title)

    if existing_page:
        # Page exists, update it
        page_id = existing_page.get('id')
        print(f"Updating the existing page {page_title}...")
        response = confluence.update_page(
            page_id=page_id,
            title=page_title,
            body=content
        )
    else:
        # Page doesn't exist, create a new page
        response = confluence.create_page(
            space=space_key,
            title=page_title,
            body=content
        )

    if response:
        print("[INFO] Confluence page created successfully!")
    else:
        print("[ERROR] Failed to create Confluence page.")



def generate_date_ranges(base_date: str, time_range: int, start_time: str, end_time: str) -> List[Tuple[str, str, str]]:
    """
    Generate a range of dates and their query periods.

    Args:
        base_date (str): The base date in YYYY-MM-DD format.
        time_range (int): Number of days before and after the base date.
        start_time (str): Start time of each day (HH:mm:ssZ format).
        end_time (str): End time of each day (HH:mm:ssZ format).

    Returns:
        List[Tuple[str, str, str]]: List of tuples containing date, start time, and end time.
    """
    base_date_obj = datetime.strptime(base_date, "%Y-%m-%d")
    start_window = base_date_obj - timedelta(days=time_range)
    end_window = base_date_obj + timedelta(days=time_range)

    return [
        (
            (start_window + timedelta(days=i)).strftime("%Y-%m-%d"),
            f"{(start_window + timedelta(days=i)).strftime('%Y-%m-%d')}T{start_time}",
            f"{(start_window + timedelta(days=i)).strftime('%Y-%m-%d')}T{end_time}"
        )
        for i in range((end_window - start_window).days + 1)
    ]


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Process log queries for multiple dates and upload results to Confluence.")
    parser.add_argument('--date', required=True, help="The base date (YYYY-MM-DD)")
    parser.add_argument('--start_time', required=True, help="Start time (HH:mm:ssZ)")
    parser.add_argument('--end_time', required=True, help="End time (HH:mm:ssZ)")
    parser.add_argument('--time_range', type=int, required=True, help="Number of days before and after the base date")
    parser.add_argument('--eu_token', required=True, help="Logz.io EU API token")
    parser.add_argument('--na_token', required=True, help="Logz.io NA API token")
    parser.add_argument('--customers_file', required=True, help="Path to customers.yaml file")
    parser.add_argument('--csv_filename', default="output.csv", help="Filename for the output CSV")
    parser.add_argument('--confluence_url', required=True, help="Base URL for Confluence instance")
    parser.add_argument('--confluence_username', required=True, help="Confluence username or email")
    parser.add_argument('--confluence_api_token', required=True, help="Confluence API token")
    parser.add_argument('--space_key', required=True, help="The key of the Confluence space")
    parser.add_argument('--page_title', required=True, help="Title for the Confluence page")

    args = parser.parse_args()

    # Fetch namespaces
    namespace_lists = fetch_namespaces(args.customers_file)

    # Generate date ranges
    date_ranges = generate_date_ranges(args.date, args.time_range, args.start_time, args.end_time)

    # Process all dates using multiprocessing
    print("[INFO] Starting multiprocessing for all dates...")
    all_date_results: Dict[str, Dict[str, Dict[str, Tuple[int, int]]]] = {}

    with Pool() as pool:
        tasks = [
            (date, namespace_lists, start_time, end_time, args.eu_token, args.na_token)
            for date, start_time, end_time in date_ranges
        ]

        for date, date_results in pool.starmap(process_date_task, tasks):
            all_date_results[date] = date_results

    print("[INFO] All processing completed!")
    # Save results to file
    log_results_and_export_to_csv(all_date_results, date_ranges, args.csv_filename)

    print("[INFO] Uploading results to Confluence...")
    create_confluence_page(
        confluence_url=args.confluence_url,
        username=args.confluence_username,
        api_token=args.confluence_api_token,
        space_key=args.space_key,
        page_title=args.page_title,
        findings=all_date_results
    )
