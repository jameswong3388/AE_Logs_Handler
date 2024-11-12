import os
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple


class JobsDataPurger:
    """A utility class for purging data from jobs CSV files."""

    def __init__(self, project_root=None):
        """Initialize the JobsDataPurger with project directory configuration."""
        self.project_root = project_root or os.path.dirname(os.path.abspath(__file__))
        self.csv_dir = os.path.join(self.project_root, 'csv')
        self.backup_dir = os.path.join(self.project_root, 'backups')

        # Ensure backup directory exists
        os.makedirs(self.backup_dir, exist_ok=True)

        # File mappings for different job files
        self.file_mappings = {
            'combined': 'combined_jobs.csv',
            'single': 'jobs.csv',
            'live': 'live_combined_jobs.csv'
        }

    def get_date_range_info(self, file_type: str = 'combined') -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        Get the available date range for the specified jobs file.

        Args:
            file_type: Type of file to check ('combined', 'single', or 'live')

        Returns:
            Tuple containing the earliest and latest dates found in the jobs data
        """
        try:
            jobs_path = os.path.join(self.csv_dir, self.file_mappings[file_type])
            if not os.path.exists(jobs_path):
                print(f"File not found: {jobs_path}")
                return None, None

            df_jobs = pd.read_csv(jobs_path)
            if df_jobs.empty:
                print("No data found in the jobs file.")
                return None, None

            earliest_date = None
            latest_date = None

            # Convert time columns to datetime
            for col in ['scheduled_time', 'start_time', 'end_time']:
                if col in df_jobs.columns:
                    df_jobs[col] = pd.to_datetime(df_jobs[col], errors='coerce')

            # Combine all datetime columns and find min/max
            dates = pd.concat([
                df_jobs['scheduled_time'].dropna(),
                df_jobs['start_time'].dropna(),
                df_jobs['end_time'].dropna()
            ])

            if not dates.empty:
                earliest_date = dates.min()
                latest_date = dates.max()

            return earliest_date, latest_date

        except Exception as e:
            print(f"Error getting date range info: {str(e)}")
            return None, None

    def create_backup(self, file_type: str):
        """
        Create a backup of the jobs file before purging.

        Args:
            file_type: Type of file to backup ('combined', 'single', or 'live')
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_subdir = os.path.join(self.backup_dir, f'{file_type}_{timestamp}')
        os.makedirs(backup_subdir, exist_ok=True)

        source_path = os.path.join(self.csv_dir, self.file_mappings[file_type])
        if os.path.exists(source_path):
            backup_path = os.path.join(backup_subdir, self.file_mappings[file_type])
            try:
                pd.read_csv(source_path).to_csv(backup_path, index=False)
                print(f"Created backup: {backup_path}")
            except Exception as e:
                print(f"Error creating backup: {str(e)}")

    def purge_data(self,
                   file_type: str,
                   start_date: datetime,
                   end_date: datetime,
                   backup: bool = True) -> int:
        """
        Purge jobs data from the specified date range.

        Args:
            file_type: Type of file to purge ('combined', 'single', or 'live')
            start_date: Start date for purging (inclusive)
            end_date: End date for purging (inclusive)
            backup: Whether to create a backup before purging

        Returns:
            Number of jobs purged
        """
        if backup:
            self.create_backup(file_type)

        jobs_purged = 0

        try:
            jobs_path = os.path.join(self.csv_dir, self.file_mappings[file_type])
            if not os.path.exists(jobs_path):
                print(f"File not found: {jobs_path}")
                return jobs_purged

            df_jobs = pd.read_csv(jobs_path)
            if df_jobs.empty:
                print("No data found in the jobs file.")
                return jobs_purged

            # Convert time columns to datetime
            for col in ['scheduled_time', 'start_time', 'end_time']:
                if col in df_jobs.columns:
                    df_jobs[col] = pd.to_datetime(df_jobs[col], errors='coerce')

            # Keep records outside the purge range
            original_len = len(df_jobs)
            df_jobs = df_jobs[
                ~(
                        ((df_jobs['scheduled_time'] >= start_date) & (df_jobs['scheduled_time'] <= end_date)) |
                        ((df_jobs['start_time'] >= start_date) & (df_jobs['start_time'] <= end_date)) |
                        ((df_jobs['end_time'] >= start_date) & (df_jobs['end_time'] <= end_date))
                )
            ]

            jobs_purged = original_len - len(df_jobs)
            df_jobs.to_csv(jobs_path, index=False)

            return jobs_purged

        except Exception as e:
            print(f"Error during purge operation: {str(e)}")
            return jobs_purged


def main():
    """Main function to run the jobs data purger interactively."""
    purger = JobsDataPurger()

    # Show available file types
    print("\nAvailable job file types:")
    print("1. Combined jobs (combined_jobs.csv)")
    print("2. Single day jobs (jobs.csv)")
    print("3. Live jobs (live_combined_jobs.csv)")

    while True:
        try:
            choice = input("\nSelect file type (1-3): ")
            file_type = {
                '1': 'combined',
                '2': 'single',
                '3': 'live'
            }[choice]
            break
        except KeyError:
            print("Invalid choice. Please select 1, 2, or 3.")

    # Show available date range
    earliest_date, latest_date = purger.get_date_range_info(file_type)
    if earliest_date and latest_date:
        print(f"\nAvailable date range in jobs data:")
        print(f"Earliest date: {earliest_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Latest date: {latest_date.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print("\nNo jobs data found or error reading date range.")
        return

    # Get date range for purging
    while True:
        try:
            start_str = input("\nEnter start date (YYYY-MM-DD HH:MM:SS): ")
            end_str = input("Enter end date (YYYY-MM-DD HH:MM:SS): ")

            start_date = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
            end_date = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')

            if start_date > end_date:
                print("Start date must be before end date.")
                continue

            break
        except ValueError:
            print("Invalid date format. Please use YYYY-MM-DD HH:MM:SS")

    # Confirm operation
    print(f"\nYou are about to purge jobs data between:")
    print(f"Start: {start_date}")
    print(f"End: {end_date}")
    print(f"File type: {file_type}")

    confirm = input("\nDo you want to continue? (yes/no): ")
    if confirm.lower() != 'yes':
        print("Operation cancelled.")
        return

    # Perform purge
    jobs_purged = purger.purge_data(file_type, start_date, end_date, backup=True)

    # Show results
    print("\nPurge operation completed!")
    print(f"Jobs purged: {jobs_purged}")
    print("\nA backup has been created in the 'backups' directory.")


if __name__ == "__main__":
    main()