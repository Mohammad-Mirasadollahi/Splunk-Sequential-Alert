import sys
import json
import requests
from requests.auth import HTTPBasicAuth
import splunklib.client as client
from urllib.parse import urlparse
import urllib3
import time

# Disable InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Function to execute AdHoc search using Splunk REST API
def execute_search(api_url, username, password, query):
    try:
        payload = {"search": query, "output_mode": "json"}
        response = requests.post(
            api_url,
            data=payload,
            auth=HTTPBasicAuth(username, password),
            verify=False
        )
        if response.status_code == 201:
            sid = response.json().get("sid")
            sys.stderr.write(f"INFO AdHoc Search job created successfully. SID: {sid}\n")
            return sid
        else:
            sys.stderr.write(f"ERROR Failed to create AdHoc Search job: {response.status_code}, {response.text}\n")
            return None
    except Exception as e:
        sys.stderr.write(f"ERROR Unexpected error during AdHoc Search execution: {e}\n")
        return None

# Function to fetch results of AdHoc search
def fetch_results(api_url, username, password, sid):
    try:
        results_url = f"{api_url}/{sid}/results?output_mode=json"
        last_log_time = time.time()  # Track the last time log was shown
        log_interval = 30  # Log interval in seconds

        while True:
            response = requests.get(results_url, auth=HTTPBasicAuth(username, password), verify=False)
            if response.status_code == 200:
                results = response.json().get("results", [])
                sys.stderr.write(f"INFO AdHoc Search results retrieved successfully.\n")
                return results
            elif response.status_code == 204:
                # Log only if the interval has passed
                current_time = time.time()
                if current_time - last_log_time >= log_interval:
                    sys.stderr.write("INFO Waiting for AdHoc Search results...\n")
                    last_log_time = current_time
            else:
                sys.stderr.write(f"ERROR Failed to fetch AdHoc Search results: {response.status_code}, {response.text}\n")
                return None
    except Exception as e:
        sys.stderr.write(f"ERROR Unexpected error during AdHoc Search results fetching: {e}\n")
        return None

# Function to execute a Saved Search (Report) using Splunk SDK
def execute_saved_search(service, report_name):
    try:
        reports = service.saved_searches
        for report in reports:
            if report.name.strip() == report_name.strip():
                app_context = report.access.app if report.access.app else "global"
                sys.stderr.write(f"INFO Saved Search '{report_name}' found in App: {app_context}. Executing...\n")
                job = report.dispatch()
                sys.stderr.write(f"INFO Saved Search executed successfully. SID: {job['sid']}\n")
                return job['sid']
        sys.stderr.write(f"ERROR Saved Search '{report_name}' not found.\n")
        return None
    except Exception as e:
        sys.stderr.write(f"ERROR Unexpected error during Saved Search execution: {e}\n")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] != "--execute":
        sys.stderr.write("FATAL Unsupported execution mode (expected --execute flag)\n")
        sys.exit(1)

    try:
        # Read configuration from stdin
        settings = json.loads(sys.stdin.read())
        query = settings['configuration'].get('query')
        username = settings['configuration'].get('username')
        password = settings['configuration'].get('password')
        api_url = settings['configuration'].get('url')
        report_name = settings['configuration'].get('report_name')

        if not username or not password or not api_url:
            sys.stderr.write("ERROR Missing required parameters (username, password, or url).\n")
            sys.exit(1)

        # Extract port from API URL
        parsed_url = urlparse(api_url)
        splunk_host = parsed_url.hostname
        splunk_port = parsed_url.port or 8089

        # Connect to Splunk for Saved Search execution
        service = client.connect(
            host=splunk_host,
            port=splunk_port,
            username=username,
            password=password
        )

        # Execute AdHoc Search if query is provided
        if query:
            sid_adhoc = execute_search(api_url, username, password, query)
            if sid_adhoc:
                fetch_results(api_url, username, password, sid_adhoc)

        # Execute Saved Search if report_name is provided
        if report_name:
            sid_saved = execute_saved_search(service, report_name)
            if not sid_saved:
                sys.stderr.write(f"ERROR Failed to execute Saved Search '{report_name}'.\n")

    except Exception as e:
        sys.stderr.write(f"ERROR Unexpected error: {e}\n")
        sys.exit(2)
