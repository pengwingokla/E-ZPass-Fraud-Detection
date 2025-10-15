# MockPass Web Scraper

A simple Python web scraper to download CSV files from the MockPass website.

## Features

- Downloads CSV files from the MockPass website
- Automatically creates timestamped filenames
- Verifies website connectivity before downloading
- Provides detailed download progress and summary
- Error handling for network issues

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Make sure the MockPass website is running:**
   ```bash
   cd ../mock-ezpass-website
   python -m http.server 8000
   ```

## Usage

### Basic Usage

Run the scraper to download all CSV files:

```bash
python csv_downloader.py
```

### What it does

1. Connects to the MockPass website at `http://localhost:8000`
2. Downloads the following CSV files:
   - `may-2025-transactions.csv`
   - `april-2025-transactions.csv`
3. Saves files with timestamps in the `downloaded_csvs` folder
4. Provides a summary of successful and failed downloads

### Output

Downloaded files are saved in the `downloaded_csvs` folder with timestamped names:
- `20251014_143022_may-2025-transactions.csv`
- `20251014_143023_april-2025-transactions.csv`

### Example Output

```
🌉 MockPass CSV Downloader
==================================================
Target URL: http://localhost:8000
Download folder: downloaded_csvs

🔍 Checking connection to http://localhost:8000...
✅ MockPass website is accessible

Downloading may-2025-transactions.csv from http://localhost:8000/may-2025-transactions.csv...
✅ Successfully downloaded may-2025-transactions.csv (295,847 bytes)
   Saved as: downloaded_csvs/20251014_143022_may-2025-transactions.csv

Downloading april-2025-transactions.csv from http://localhost:8000/april-2025-transactions.csv...
✅ Successfully downloaded april-2025-transactions.csv (378,234 bytes)
   Saved as: downloaded_csvs/20251014_143023_april-2025-transactions.csv

==================================================
📊 Download Summary:
   Total files: 2
   Successful: 2
   Failed: 0

✅ Successfully downloaded:
   - may-2025-transactions.csv
   - april-2025-transactions.csv
```

## Customization

You can modify the script to:

- Change the base URL (if running on a different port)
- Add more CSV files to download
- Change the download folder location
- Modify the timestamping format

## Troubleshooting

**"Cannot connect to MockPass website"**
- Make sure the Python web server is running in the mock-ezpass-website folder
- Check that port 8000 is not blocked by firewall
- Verify the URL is correct

**"Failed to download"**
- Check that the CSV files exist in the mock-ezpass-website folder
- Ensure you have write permissions in the current directory
- Check your internet connection (if using a remote URL)

## Files

- `csv_downloader.py` - Main scraper script
- `requirements.txt` - Python dependencies
- `downloaded_csvs/` - Folder where CSV files are saved (created automatically)