import os
import time

from src.utils import (
    PROJECT_ROOT, parse_sap_jobs, save_jobs_to_csv,
    save_benchmarks, read_log_file, extract_time_range
)


def process_log_to_csv(log_file_path):
    """Process a single log file and extract job information."""
    start_time = time.time()

    # Read and validate log file
    log_content = read_log_file(log_file_path)
    if not log_content:
        print(f"Error: Could not read log file {log_file_path}")
        return

    # Extract and display time range
    log_start_time, log_end_time = extract_time_range(log_content)
    if log_start_time and log_end_time:
        print(f"\nLog period: {log_start_time} to {log_end_time}")

    # Parse jobs from log content
    jobs = parse_sap_jobs(log_content)
    print(f"Found {len(jobs)} jobs in log file")

    # Save jobs to CSV
    job_headers = ['id', 'name', 'scheduled_time', 'start_time', 'end_time', 'return_code',
                   'scheduled_message_code', 'start_message_code', 'end_message_code', 'remove_message_code']
    save_jobs_to_csv(jobs, 'jobs.csv', job_headers)

    # Calculate processing metrics
    processing_time = time.time() - start_time

    # Print processing summary
    print(f"\nProcessing complete!")
    print(f"Data has been saved to csv/jobs.csv")
    print(f"Processing time: {processing_time:.2f} seconds")

    # Save performance benchmarks
    benchmarks = [
        (os.path.basename(log_file_path), processing_time)
    ]
    save_benchmarks(benchmarks, 'single_benchmarks.csv')
    print("Processing time has been saved to benchmarks/single_benchmarks.csv")

    return jobs, processing_time


if __name__ == "__main__":
    log_file_path = os.path.join(PROJECT_ROOT, 'logs', '189229440.LOG.txt')
    process_log_to_csv(log_file_path)