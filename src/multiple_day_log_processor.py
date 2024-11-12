import os
import time
from src.utils import (
    PROJECT_ROOT, extract_time_range, parse_sap_jobs, save_jobs_to_csv,
    save_benchmarks, read_log_file
)


def process_log_file(log_file_path, filename):
    """Process a single log file and extract job information."""
    file_start_time = time.time()

    log_content = read_log_file(log_file_path)
    if not log_content:
        return 0, 0

    start_time, end_time = extract_time_range(log_content)
    if start_time and end_time:
        print(f"Log period: {start_time} to {end_time}")
    else:
        print("Unable to extract time range from the log file.")
        print(f"File size: {os.path.getsize(log_file_path)} bytes")
        print(f"First 100 characters: {log_content[:100]}")

    # Parse jobs from log content
    jobs = parse_sap_jobs(log_content)

    # Save jobs to CSV
    job_headers = ['id', 'name', 'scheduled_time', 'start_time', 'end_time', 'return_code',
                   'scheduled_message_code', 'start_message_code', 'end_message_code', 'remove_message_code']
    save_jobs_to_csv(jobs, 'combined_jobs.csv', job_headers, mode='a')

    # Calculate processing time
    file_processing_time = time.time() - file_start_time

    print(f"Processed {filename} in {file_processing_time:.2f} seconds")
    print(f"Found {len(jobs)} jobs in log file")

    return file_processing_time, len(jobs)


def process_logs_to_csv(logs_folder):
    """Process multiple log files and combine job data."""
    processing_times = []
    total_jobs = 0

    total_start_time = time.time()
    logs_path = os.path.join(PROJECT_ROOT, logs_folder)

    # Clear existing jobs CSV file
    jobs_csv_path = os.path.join(PROJECT_ROOT, 'csv', 'combined_jobs.csv')
    if os.path.exists(jobs_csv_path):
        os.remove(jobs_csv_path)

    # Process each log file
    for filename in os.listdir(logs_path):
        if filename.endswith('.LOG.txt'):
            log_file_path = os.path.join(logs_path, filename)
            print(f"\nProcessing file: {filename}")

            file_processing_time, num_jobs = process_log_file(log_file_path, filename)

            processing_times.append((filename, file_processing_time))
            total_jobs += num_jobs

    # Calculate final metrics
    total_end_time = time.time()
    total_processing_time = total_end_time - total_start_time

    print(f"\nProcessing complete!")
    print(f"Total processing time: {total_processing_time:.2f} seconds")
    print(f"Total jobs processed: {total_jobs}")
    print(f"\nCombined jobs data has been saved to csv/combined_jobs.csv")

    # Save performance benchmarks
    benchmarks = [
        (filename, process_time)
        for filename, process_time in processing_times
    ]
    benchmarks.append(('Total', total_processing_time))
    benchmarks.append(('Average', total_processing_time / len(processing_times) if processing_times else 0))

    save_benchmarks(benchmarks, 'multiple_benchmarks.csv')
    print("Processing times have been saved to benchmarks/multiple_benchmarks.csv")


if __name__ == "__main__":
    logs_folder = 'logs'
    process_logs_to_csv(logs_folder)