import os
import pandas as pd


class JobAnalyzer:
    def __init__(self, project_root=None):
        """Initialize the Job Analyzer."""
        self.project_root = project_root or os.path.dirname(os.path.abspath(__file__))
        self.jobs_df = None

    def load_data(self):
        """Load jobs data from CSV file."""
        print("Loading data files...")

        # Load DataFrame
        self.jobs_df = pd.read_csv(os.path.join(self.project_root, 'csv', 'combined_jobs.csv'))

        # Convert time columns to datetime
        time_columns = ['scheduled_time', 'start_time', 'end_time']
        for col in time_columns:
            if col in self.jobs_df.columns:
                self.jobs_df[col] = pd.to_datetime(self.jobs_df[col], format='%Y-%m-%d %H:%M:%S', errors='coerce')

        print("Data loading complete.")

    def analyze_jobs(self):
        """
        Analyze jobs to determine concurrent jobs and identify potential issues.
        """
        if self.jobs_df is None or self.jobs_df.empty:
            print("No job data available. Please load data first.")
            return None, None, []

        # Calculate job durations
        mask = self.jobs_df['start_time'].notna() & self.jobs_df['end_time'].notna()
        self.jobs_df.loc[mask, 'duration'] = (
                                                     self.jobs_df.loc[mask, 'end_time'] - self.jobs_df.loc[
                                                 mask, 'start_time']
                                             ).dt.total_seconds() / 60  # Duration in minutes

        # Find concurrent jobs
        events = []
        for _, row in self.jobs_df.iterrows():
            if pd.notnull(row['start_time']):
                events.append((row['start_time'], 1, row['name'], row['id']))
            if pd.notnull(row['end_time']):
                events.append((row['end_time'], -1, row['name'], row['id']))

        if not events:
            print("No valid job timing data found for analysis.")
            return None, None, []

        # Sort events by timestamp
        events.sort(key=lambda x: x[0])

        # Calculate concurrent jobs over time
        concurrent_jobs = []
        active_jobs = {}

        for time, change, job_name, job_id in events:
            if change == 1:
                active_jobs[job_id] = job_name
            else:
                active_jobs.pop(job_id, None)

            concurrent_jobs.append({
                'timestamp': time,
                'concurrent_jobs': len(active_jobs),
                'active_jobs': ', '.join(active_jobs.values())
            })

        # Create DataFrame from concurrent jobs data
        concurrent_df = pd.DataFrame(concurrent_jobs) if concurrent_jobs else None

        # Find longest running job
        valid_duration_mask = (self.jobs_df['duration'] > 0) & (self.jobs_df['duration'] < 1440)  # Max 24 hours
        longest_job = self.jobs_df[valid_duration_mask].nlargest(1, 'duration')

        # Check if the longest job had issues
        issues = []
        if not longest_job.empty:
            job = longest_job.iloc[0]

            # Check return code
            if pd.notnull(job['return_code']) and str(job['return_code']) != '0':
                issues.append(f"Job failed with return code: {job['return_code']}")

            # Check if job took unusually long (more than 4 hours)
            if job['duration'] > 240:  # 4 hours in minutes
                issues.append(f"Job took unusually long: {job['duration']:.1f} minutes")

            # Check if job started at scheduled time
            if pd.notnull(job['scheduled_time']) and pd.notnull(job['start_time']):
                delay = (job['start_time'] - job['scheduled_time']).total_seconds() / 60
                if delay > 30:  # If started more than 30 minutes late
                    issues.append(f"Job started {delay:.1f} minutes later than scheduled")

        return concurrent_df, longest_job, issues


def main():
    analyzer = JobAnalyzer()

    try:
        # Load data
        analyzer.load_data()

        # Analyze all jobs
        print("\nAnalyzing all jobs...")
        concurrent_jobs, longest_job, issues = analyzer.analyze_jobs()

        if concurrent_jobs is not None:
            max_concurrent = concurrent_jobs['concurrent_jobs'].max()
            print(f"\nMaximum concurrent jobs: {max_concurrent}")

            # Show details for periods with maximum concurrent jobs
            max_periods = concurrent_jobs[concurrent_jobs['concurrent_jobs'] == max_concurrent]
            print(f"Time periods with {max_concurrent} concurrent jobs:")
            for _, row in max_periods.iterrows():
                print(f"- {row['timestamp']}")
                print(f"  Running jobs: {row['active_jobs']}")

        if longest_job is not None and not longest_job.empty:
            duration_mins = longest_job.iloc[0]['duration']
            hours = int(duration_mins // 60)
            mins = int(duration_mins % 60)
            print(f"\nLongest running job:")
            print(f"Name: {longest_job.iloc[0]['name']}")
            print(f"Start time: {longest_job.iloc[0]['start_time']}")
            print(f"End time: {longest_job.iloc[0]['end_time']}")
            print(f"Duration: {hours}h {mins}m")
            print(f"Job ID: {longest_job.iloc[0]['id']}")

            if issues:
                print("\nPotential issues detected:")
                for issue in issues:
                    print(f"- {issue}")
            else:
                print("\nNo issues detected with longest job.")
        else:
            print("\nNo valid job duration data found.")

    except Exception as e:
        print(f"\nError during analysis: {str(e)}")
        raise


if __name__ == "__main__":
    main()

