#!/usr/bin/env python3
"""
MockPass CSV Downloader
A simple web scraper to download CSV files from the MockPass website
"""

import requests
import os
import time
from datetime import datetime
from urllib.parse import urljoin

class MockPassDownloader:
    def __init__(self, base_url="http://localhost:8000"):
        """
        Initialize the downloader with the MockPass website URL
        
        Args:
            base_url (str): Base URL of the MockPass website
        """
        self.base_url = base_url
        self.download_folder = "downloaded_csvs"
        self.csv_files = [
            "data/may-2025-transactions.csv",
            "data/april-2025-transactions.csv"
        ]
        
        # Create download folder if it doesn't exist
        if not os.path.exists(self.download_folder):
            os.makedirs(self.download_folder)
    
    def download_csv(self, filename):
        """
        Download a specific CSV file from the MockPass website
        
        Args:
            filename (str): Name of the CSV file to download
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Construct the full URL
            file_url = urljoin(self.base_url, filename)
            
            print(f"Downloading {filename} from {file_url}...")
            
            # Send GET request
            response = requests.get(file_url, timeout=30)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            # Generate timestamped filename - extract just the filename without path
            base_filename = os.path.basename(filename)  # Extract filename from path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            local_filename = f"{timestamp}_{base_filename}"
            local_path = os.path.join(self.download_folder, local_filename)
            
            # Save the file
            with open(local_path, 'wb') as f:
                f.write(response.content)
            
            file_size = len(response.content)
            print(f"✅ Successfully downloaded {filename} ({file_size:,} bytes)")
            print(f"   Saved as: {local_path}")
            
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error downloading {filename}: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error downloading {filename}: {e}")
            return False
    
    def download_all_csvs(self):
        """
        Download all CSV files from the MockPass website
        
        Returns:
            dict: Summary of download results
        """
        print("🌉 MockPass CSV Downloader")
        print("=" * 50)
        print(f"Target URL: {self.base_url}")
        print(f"Download folder: {self.download_folder}")
        print()
        
        results = {
            'successful': [],
            'failed': [],
            'total_files': len(self.csv_files)
        }
        
        for filename in self.csv_files:
            if self.download_csv(filename):
                results['successful'].append(filename)
            else:
                results['failed'].append(filename)
            
            # Small delay between downloads
            time.sleep(1)
        
        # Print summary
        print("\n" + "=" * 50)
        print("📊 Download Summary:")
        print(f"   Total files: {results['total_files']}")
        print(f"   Successful: {len(results['successful'])}")
        print(f"   Failed: {len(results['failed'])}")
        
        if results['successful']:
            print(f"\n✅ Successfully downloaded:")
            for file in results['successful']:
                print(f"   - {file}")
        
        if results['failed']:
            print(f"\n❌ Failed to download:")
            for file in results['failed']:
                print(f"   - {file}")
        
        return results
    
    def verify_website_connection(self):
        """
        Verify that the MockPass website is accessible
        
        Returns:
            bool: True if website is accessible, False otherwise
        """
        try:
            print(f"🔍 Checking connection to {self.base_url}...")
            response = requests.get(self.base_url, timeout=10)
            response.raise_for_status()
            
            if "MockPass" in response.text:
                print("✅ MockPass website is accessible")
                return True
            else:
                print("⚠️ Website accessible but may not be MockPass")
                return True
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Cannot connect to MockPass website: {e}")
            print("💡 Make sure the Python web server is running:")
            print("   cd mock-ezpass-website")
            print("   python -m http.server 8000")
            return False


def main():
    """Main function to run the CSV downloader"""
    
    # Initialize downloader
    downloader = MockPassDownloader()
    
    # Check if website is accessible
    if not downloader.verify_website_connection():
        print("\n🚫 Cannot proceed - website not accessible")
        return
    
    print()
    
    # Download all CSV files
    results = downloader.download_all_csvs()
    
    # Exit with appropriate code
    if results['failed']:
        exit(1)  # Some downloads failed
    else:
        exit(0)  # All downloads successful


if __name__ == "__main__":
    main()