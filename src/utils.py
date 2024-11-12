import csv
import os
import re
from collections import defaultdict
from datetime import datetime

import psutil

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

def extract_time_range(log_content):
    """Extract the time range from the log content."""
    lines = log_content.split('\n')
    timestamp_pattern = r'(\d{8}/\d{6}\.\d{3})'

    start_time = None
    end_time = None

    # Find the first line with a timestamp
    for line in lines:
        match = re.search(timestamp_pattern, line)
        if match:
            start_time = datetime.strptime(match.group(1), '%Y%m%d/%H%M%S.%f')
            break

    # Find the last line with a timestamp
    for line in reversed(lines):
        match = re.search(timestamp_pattern, line)
        if match:
            end_time = datetime.strptime(match.group(1), '%Y%m%d/%H%M%S.%f')
            break

    if not start_time:
        print("Debug: Unable to extract start time.")
        print("First few lines:", '\n'.join(lines[:5]))
    if not end_time:
        print("Debug: Unable to extract end time.")
        print("Last few lines:", '\n'.join(lines[-5:]))

    return start_time, end_time

def parse_sap_jobs(log_content):
    """Parse SAP log content for jobs information."""
    jobs = defaultdict(lambda: defaultdict(str))

    patterns = {
        'timestamp': r'(\d{8}/\d{6}\.\d{3})',
        'message_code': r'(U\d{8})',
        'job_is_to_be_started': r'Job \'(.+?)\' with RunID \'(\d+)\' is to be started\.',
        'job_start': r'Job \'(.+?)\' started with RunID \'(\d+)\'\.',
        'job_end': r'Job \'(.+?)\' with RunID \'(\d+)\' ended with return code \'(\d+)\'.',
        'job_remove': r'Job \'(.+?)\' with RunID \'(\d+)\' has been removed from the job table.'
    }

    for line in log_content.split('\n'):
        timestamp_match = re.search(patterns['timestamp'], line)
        message_code_match = re.search(patterns['message_code'], line)

        if timestamp_match and message_code_match:
            timestamp = datetime.strptime(timestamp_match.group(1), '%Y%m%d/%H%M%S.%f')
            message_code = message_code_match.group(1)

            for pattern_name, pattern in patterns.items():
                if pattern_name not in ['timestamp', 'message_code']:
                    match = re.search(pattern, line)
                    if match:
                        if pattern_name == 'job_is_to_be_started':
                            job_name, run_id = match.groups()
                            jobs[run_id].update({
                                'name': job_name,
                                'scheduled_time': timestamp,
                                'scheduled_message_code': message_code
                            })
                        elif pattern_name == 'job_start':
                            job_name, run_id = match.groups()
                            jobs[run_id].update({
                                'name': job_name,
                                'start_time': timestamp,
                                'start_message_code': message_code
                            })
                        elif pattern_name == 'job_end':
                            job_name, run_id, return_code = match.groups()
                            jobs[run_id].update({
                                'name': job_name,
                                'return_code': return_code,
                                'end_message_code': message_code
                            })
                        elif pattern_name == 'job_remove':
                            job_name, run_id = match.groups()
                            jobs[run_id].update({
                                'name': job_name,
                                'end_time': timestamp,
                                'remove_message_code': message_code
                            })
                        break

    return jobs

def save_jobs_to_csv(jobs, filename, headers, mode='w'):
    """Save jobs data to CSV file."""
    filepath = os.path.join(PROJECT_ROOT, 'csv', filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    file_exists = os.path.isfile(filepath)

    with open(filepath, mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        if not file_exists or mode == 'w':
            writer.writeheader()
        for key, value in jobs.items():
            row = {'id': key}
            row.update(value)
            writer.writerow(row)

def monitor_resources():
    """Monitor CPU and RAM usage."""
    process = psutil.Process()
    return process.cpu_percent(), process.memory_info().rss / (1024 * 1024)  # CPU % and RAM in MB

def save_benchmarks(benchmarks, filename):
    """Save performance benchmarks to CSV file."""
    filepath = os.path.join(PROJECT_ROOT, 'benchmarks', filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['File', 'Processing Time (seconds)', 'CPU Usage (%)', 'RAM Usage (MB)'])
        writer.writerows(benchmarks)

def read_log_file(log_file_path):
    """Read log file with multiple encoding support."""
    encodings = ['utf-8', 'iso-8859-1', 'windows-1252', 'ascii']
    for encoding in encodings:
        try:
            with open(log_file_path, 'r', encoding=encoding) as file:
                return file.read()
        except UnicodeDecodeError:
            continue
    print(f"Error: Unable to decode file {log_file_path} with any of the attempted encodings.")
    return None

def create_or_clear_csv(filename):
    """Create a new CSV file or clear existing content."""
    filepath = os.path.join(PROJECT_ROOT, 'csv', filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    open(filepath, 'w').close()