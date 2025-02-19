# csv_merger

## Overview
`csv_merger` is a Python script that combines multiple CSV files into one. It detects file encodings and handles errors during merging.

## Prerequisites
- Python 3.7+
- Install dependencies: `pip install pandas chardet`

## Directory Structure
- `py_tomerge`: Store CSV files for merging.
- `py_mergedcsv`: Output directory for the merged file.

## Usage
1. Put CSV files in `py_tomerge`.
2. Run `python csv_merger_new.py` in the script directory.
3. Find the merged `merged.csv` in `py_mergedcsv`.

## Code Explanation
- `detect_encoding`: Detects file encoding.
- `get_file_path`: Forms a full file path.
- `merge_csv_files`: Main function for merging, including directory check, file reading, deduplication, and saving.

## Logging
Uses Python's `logging` module with console output. Configure `filename` in `logging.basicConfig` to save logs to a file.

## Error Handling
Skips files with encoding or parsing errors and logs the issues.

## Future Improvements
- Add command-line arguments for input/output directories and file names.
- Implement advanced filtering options for the merged DataFrame.