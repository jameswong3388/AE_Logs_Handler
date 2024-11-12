# AE Log Handler

## Project Overview

The AE Log Handler project is a Python-based tool suite designed to process and analyze SAP system log files. It provides comprehensive functionality for parsing log files, extracting job information, and generating detailed analytics in both batch and real-time modes.

## Project Structure

```
root
├── src
│   ├── csv            # Output CSV files for parsed data
│   │   ├── jobs.csv              # Single day job data
│   │   ├── combined_jobs.csv     # Multiple day combined job data
│   │   └── live_combined_jobs.csv # Real-time job data
│   ├── logs           # Input directory for batch processing
│   ├── live_logs      # Input directory for real-time processing
│   ├── backups        # Backup directory for job data
│   ├── benchmarks     # Performance metrics
│   ├── data_purger.py
│   ├── jobs_analyzer.py
│   ├── live_log_processor.py
│   ├── multiple_day_log_processor.py
│   ├── single_day_log_processor.py
│   └── utils.py       # Common utilities and helper functions
└── README.md
```

## Key Components

### 1. Data Purger (`data_purger.py`)
- **Job Data Management**:
  - Selective data purging based on date ranges
  - Support for multiple job file types (combined, single, live)
  - Automatic backup creation before purging
  - Date range validation
  - Interactive command-line interface

### 2. Jobs Analyzer (`jobs_analyzer.py`)
- **Job Analysis Features**:
  - Concurrent job detection and analysis
  - Job duration calculations
  - Start/end time monitoring
  - Return code verification
  - Issue detection and reporting
  - Resource utilization tracking

### 3. Live Log Processor (`live_log_processor.py`)
- Real-time log file monitoring
- Automatic processing of new logs
- Resource usage tracking
- Performance benchmarking
- Duplicate processing prevention
- File modification handling

### 4. Multiple Day Log Processor (`multiple_day_log_processor.py`)
- Batch processing capabilities
- Job extraction and consolidation
- Processing time tracking
- Performance benchmarking
- Combined CSV output generation

### 5. Single Day Log Processor (`single_day_log_processor.py`)
- Individual log file processing
- Time range extraction
- Job information parsing
- CSV output generation
- Processing metrics recording

### 6. Utilities (`utils.py`)
- Enhanced log parsing with multiple encoding support
- Time range extraction
- SAP job parsing
- CSV operations
- Resource monitoring
- Performance benchmarking

## Features

### Data Processing
- **Log Parsing**:
  - Multiple encoding support (utf-8, iso-8859-1, windows-1252, ascii)
  - Robust error handling
  - Pattern-based job extraction
  - Timestamp validation

- **Job Analysis**:
  - Duration calculation
  - Concurrent job detection
  - Status tracking
  - Return code analysis
  - Issue identification

- **Data Management**:
  - Automated backups
  - Selective data purging
  - Date range validation
  - Multiple file type support

### Analysis Capabilities
- **Job Metrics**:
  - Concurrent job analysis
  - Processing duration tracking
  - Return code verification
  - Start/end time monitoring
  - Issue detection

- **System Analysis**:
  - Resource utilization monitoring
  - Performance benchmarking
  - Processing time tracking
  - Memory usage analysis

## Usage Instructions

### 1. Data Purging
```bash
python src/data_purger.py
```
- Interactive interface for selecting file type
- Date range specification
- Automatic backup creation
- Purge confirmation

### 2. Real-time Processing
```bash
python src/live_log_processor.py
```
- Monitors `src/live_logs` directory
- Real-time processing and analysis
- Resource usage tracking
- Benchmark generation

### 3. Batch Processing
Single log file:
```bash
python src/single_day_log_processor.py
```

Multiple log files:
```bash
python src/multiple_day_log_processor.py
```

### 4. Job Analysis
```bash
python src/jobs_analyzer.py
```

### Output Locations
- Job data: `src/csv/`
- Backups: `src/backups/`
- Performance metrics: `src/benchmarks/`

## Requirements

### Python Dependencies
```
pandas>=1.3.0
watchdog>=2.1.0
psutil>=5.8.0
```

### Installation
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Best Practices

### 1. Data Management
- Regular backups before data purging
- Periodic review of backup directory
- Monitoring of disk space usage
- Validate date ranges before purging

### 2. Performance Optimization
- Process large batches during off-peak hours
- Monitor resource utilization
- Regular benchmark review
- Clean up old backups periodically

### 3. Analysis Workflow
- Review concurrent job patterns
- Monitor return codes
- Track processing times
- Validate job completions

## Contributing
1. Fork the repository
2. Create a feature branch
3. Implement changes with tests
4. Submit pull request with documentation

### Code Standards
- PEP 8 compliance
- Type hints for function parameters
- Comprehensive error handling
- Clear documentation
- Error logging

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.