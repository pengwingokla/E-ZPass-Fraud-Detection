#!/usr/bin/env python3
"""
MockPass CSV Downloader with Automated Login
A web scraper that automatically logs into MockPass and downloads CSV files
"""

import os
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException

class MockPassDownloader:
    def __init__(self, base_url="http://localhost:8000", username="ezpass", password="ezpass", headless=True):
        """
        Initialize the downloader with the MockPass website URL and credentials
        
        Args:
            base_url (str): Base URL of the MockPass website
            username (str): Login username
            password (str): Login password
            headless (bool): Run browser in headless mode (no GUI)
        """
        self.base_url = base_url
        self.username = username
        self.password = password
        self.headless = headless
        self.download_folder = os.path.abspath("downloaded_csvs")
        self.driver = None
        self.csv_files = [
            "data/may-2025-transactions.csv",
            "data/april-2025-transactions.csv"
        ]
        
        # Create download folder if it doesn't exist
        if not os.path.exists(self.download_folder):
            os.makedirs(self.download_folder)
    
    def setup_driver(self):
        """
        Set up Selenium WebDriver with Chrome/Edge
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            print("🔧 Setting up browser driver...")
            
            # Chrome options
            chrome_options = Options()
            if self.headless:
                chrome_options.add_argument('--headless=new')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            
            # Set download preferences
            prefs = {
                "download.default_directory": self.download_folder,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            }
            chrome_options.add_experimental_option("prefs", prefs)
            
            # Try Chrome first, then Edge
            try:
                self.driver = webdriver.Chrome(options=chrome_options)
                print("✅ Chrome driver initialized")
            except (WebDriverException, Exception) as e:
                print(f"⚠️ Chrome not available, trying Edge... ({e})")
                from selenium.webdriver.edge.options import Options as EdgeOptions
                edge_options = EdgeOptions()
                if self.headless:
                    edge_options.add_argument('--headless=new')
                edge_options.add_argument('--no-sandbox')
                edge_options.add_argument('--disable-dev-shm-usage')
                edge_options.add_experimental_option("prefs", prefs)
                
                self.driver = webdriver.Edge(options=edge_options)
                print("✅ Edge driver initialized")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to initialize browser driver: {e}")
            print("💡 Make sure Chrome or Edge is installed")
            print("   Install selenium: pip install selenium")
            return False
    
    def login_to_mockpass(self):
        """
        Automate login to MockPass website
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            print(f"🔐 Logging into MockPass at {self.base_url}...")
            
            # Navigate to login page
            self.driver.get(f"{self.base_url}/login.html")
            time.sleep(2)
            
            # Click the main login button to open modal
            print("   Opening login modal...")
            login_btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "main-login-btn"))
            )
            login_btn.click()
            time.sleep(1)
            
            # Wait for modal to appear
            modal = WebDriverWait(self.driver, 10).until(
                EC.visibility_of_element_located((By.ID, "register_box_front"))
            )
            print("   Modal opened")
            
            # Fill username
            username_field = self.driver.find_element(By.ID, "modal-username")
            username_field.clear()
            username_field.send_keys(self.username)
            print(f"   Entered username: {self.username}")
            
            # Fill password
            password_field = self.driver.find_element(By.ID, "modal-password")
            password_field.clear()
            password_field.send_keys(self.password)
            print(f"   Entered password: {'*' * len(self.password)}")
            
            # Get captcha value and fill it
            captcha_element = self.driver.find_element(By.ID, "modal-captcha-value")
            captcha_value = captcha_element.text.strip()
            print(f"   Captcha detected: {captcha_value}")
            
            captcha_input = self.driver.find_element(By.ID, "modal-captcha-input")
            captcha_input.clear()
            captcha_input.send_keys(captcha_value)
            print(f"   Entered captcha: {captcha_value}")
            
            # Submit the form
            submit_btn = self.driver.find_element(By.CSS_SELECTOR, ".modal-login-btn")
            submit_btn.click()
            print("   Submitting login form...")
            
            # Wait for redirect to index.html (successful login)
            WebDriverWait(self.driver, 10).until(
                EC.url_contains("index.html")
            )
            
            print("✅ Successfully logged in!")
            time.sleep(2)  # Wait for page to fully load
            return True
            
        except TimeoutException:
            print("❌ Login timeout - elements not found or login failed")
            return False
        except Exception as e:
            print(f"❌ Login error: {e}")
            return False
    
    def download_csv_with_selenium(self, filename):
        """
        Download a CSV file using Selenium by navigating directly to the file URL
        
        Args:
            filename (str): Name of the CSV file to download
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Extract just the filename without path for display
            base_filename = os.path.basename(filename)
            
            print(f"📥 Downloading {filename}...")
            
            # Construct full URL
            file_url = f"{self.base_url}/{filename}"
            
            # Navigate to the CSV file URL - the browser will download it or display it
            self.driver.get(file_url)
            time.sleep(2)
            
            # Get page source (which contains the CSV content if it's displayed)
            page_content = self.driver.page_source
            
            # Check if it's CSV content (starts with typical CSV headers or data)
            # If the browser rendered it as text, extract it
            if "<!DOCTYPE html>" not in page_content[:100].lower():
                # It's plain text/CSV content shown in browser
                csv_content = page_content
            else:
                # It's wrapped in HTML, try to extract from <pre> tag
                try:
                    pre_element = self.driver.find_element(By.TAG_NAME, "pre")
                    csv_content = pre_element.text
                except:
                    print(f"❌ Could not extract CSV content from {filename}")
                    return False
            
            # Generate timestamped filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_filename = f"{timestamp}_{base_filename}"
            file_path = os.path.join(self.download_folder, new_filename)
            
            # Save the CSV content
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(csv_content)
            
            file_size = os.path.getsize(file_path)
            print(f"✅ Successfully downloaded {filename} ({file_size:,} bytes)")
            print(f"   Saved as: {file_path}")
            return True
                
        except Exception as e:
            print(f"❌ Error downloading {filename}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def download_all_csvs(self):
        """
        Download all CSV files from the MockPass website with automated login
        
        Returns:
            dict: Summary of download results
        """
        print("🌉 MockPass Automated CSV Downloader")
        print("=" * 50)
        print(f"Target URL: {self.base_url}")
        print(f"Download folder: {self.download_folder}")
        print(f"Username: {self.username}")
        print()
        
        results = {
            'successful': [],
            'failed': [],
            'total_files': len(self.csv_files)
        }
        
        try:
            # Setup browser driver
            if not self.setup_driver():
                print("❌ Failed to setup browser driver")
                return results
            
            # Login to MockPass
            if not self.login_to_mockpass():
                print("❌ Failed to login to MockPass")
                return results
            
            # Download each CSV file
            for filename in self.csv_files:
                if self.download_csv_with_selenium(filename):
                    results['successful'].append(filename)
                else:
                    results['failed'].append(filename)
                
                # Small delay between downloads
                time.sleep(1)
        
        finally:
            # Clean up: close browser
            if self.driver:
                print("\n🔒 Closing browser...")
                self.driver.quit()
        
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
            import requests
            
            print(f"🔍 Checking connection to {self.base_url}...")
            response = requests.get(self.base_url, timeout=10)
            response.raise_for_status()
            
            if "MockPass" in response.text or "login" in response.text.lower():
                print("✅ MockPass website is accessible")
                return True
            else:
                print("⚠️ Website accessible but may not be MockPass")
                return True
                
        except Exception as e:
            print(f"❌ Cannot connect to MockPass website: {e}")
            print("💡 Make sure the Python web server is running:")
            print("   cd mock-ezpass-website")
            print("   python -m http.server 8000")
            return False


def main():
    """Main function to run the automated CSV downloader"""
    
    # You can customize these parameters
    BASE_URL = "http://localhost:8000"
    USERNAME = "ezpass"
    PASSWORD = "ezpass"
    HEADLESS = False  # Set to True to hide browser window
    
    # Initialize downloader
    downloader = MockPassDownloader(
        base_url=BASE_URL,
        username=USERNAME,
        password=PASSWORD,
        headless=HEADLESS
    )
    
    # Check if website is accessible
    if not downloader.verify_website_connection():
        print("\n🚫 Cannot proceed - website not accessible")
        return
    
    print()
    
    # Download all CSV files with automated login
    results = downloader.download_all_csvs()
    
    # Exit with appropriate code
    if results['failed']:
        exit(1)  # Some downloads failed
    else:
        print("\n🎉 All downloads completed successfully!")
        exit(0)  # All downloads successful


if __name__ == "__main__":
    main()