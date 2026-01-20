import os
import time
import random
import logging
import pandas as pd
from pathlib import Path
from typing import List, Optional, Set, Dict
from urllib.parse import urlparse
import undetected_chromedriver as uc 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True,
)


class OpenAlexFileDownloader:
    """A class to download files from OpenAlex URLs using Selenium WebDriver."""

    DEFAULT_TIMEOUT = 30
    PAGE_LOAD_WAIT = 2

    MIN_DELAY = 2.0
    MAX_DELAY = 5.0
    MIN_PAGE_DELAY = 3.0
    MAX_PAGE_DELAY = 8.0
    
    # New constant for manual captcha solving
    CAPTCHA_WAIT_TIME = 300 

    def __init__(
        self,
        urls: List[str],
        download_directory: Optional[str] = "./openalex_downloads",
        headless: bool = False,
        random_delay: bool = True,
    ):
        self.urls = urls
        self.download_directory = download_directory
        self.headless = headless
        self.random_delay = random_delay
        self.driver = None
        self.downloaded_files: Set[str] = set()

        if not isinstance(self.download_directory, str):
            raise ValueError("download_directory must be a string")

        Path(self.download_directory).mkdir(parents=True, exist_ok=True)
        logging.info(f"Download directory set to: {self.download_directory}")

        self._update_downloaded_files()
        logging.info(f"Initial files in directory: {len(self.downloaded_files)}")

        self._setup_driver()

    def _random_delay(self, min_seconds: float = None, max_seconds: float = None):
        if not self.random_delay:
            return

        min_delay = min_seconds if min_seconds is not None else self.MIN_DELAY
        max_delay = max_seconds if max_seconds is not None else self.MAX_DELAY

        delay = random.uniform(min_delay, max_delay)
        logging.info(f"Adding random delay: {delay:.2f}s")
        time.sleep(delay)

    def _update_downloaded_files(self):
        try:
            download_path = Path(self.download_directory)
            current_files = {f.name for f in download_path.glob("*.pdf") if f.is_file()}
            self.downloaded_files = current_files
        except Exception as e:
            logging.error(f"Error updating downloaded files: {str(e)}")
            self.downloaded_files = set()

    def _check_download_status(
        self, timeout: int = 60, check_interval: float = 1.0
    ) -> bool:
        download_path = Path(self.download_directory)
        start_time = time.time()

        initial_files = self.downloaded_files.copy()
        logging.info(f"Monitoring for new downloads (timeout: {timeout}s)...")

        while time.time() - start_time < timeout:
            temp_files = list(download_path.glob("*.crdownload"))

            if temp_files:
                logging.info(f"Download in progress: {temp_files[0].name}")
                time.sleep(check_interval)
                continue

            current_files = {f.name for f in download_path.glob("*.pdf") if f.is_file()}
            new_files = current_files - initial_files

            if new_files:
                new_file = list(new_files)[0]
                file_path = download_path / new_file

                if file_path.exists() and file_path.stat().st_size > 0:
                    file_size = file_path.stat().st_size
                    logging.info(
                        f"New download detected: {new_file} ({file_size:,} bytes)"
                    )

                    self.downloaded_files = current_files
                    return True

            time.sleep(check_interval)

        logging.warning(f"Download check timeout after {timeout} seconds")
        return False

    def _setup_driver(self):
        # Using uc.ChromeOptions for better stealth
        chrome_options = uc.ChromeOptions()

        abs_download_dir = os.path.abspath(self.download_directory)
        logging.info(f"Setting download directory to: {abs_download_dir}")

        if self.headless:
            chrome_options.add_argument("--headless")

        # Basic stability and download arguments
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # This helps skip the "Welcome to Chrome" popups that block downloads
        chrome_options.add_argument("--no-first-run")
        chrome_options.add_argument("--no-service-autorun")
        chrome_options.add_argument("--password-store=basic")

        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]
        chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")

        prefs = {
            "download.default_directory": abs_download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
            "profile.default_content_settings.popups": 0,
            "profile.default_content_setting_values.automatic_downloads": 1,
        }
        chrome_options.add_experimental_option("prefs", prefs)

        logging.info("Initializing Undetected ChromeDriver...")
        self.driver = uc.Chrome(options=chrome_options)
        
        # FIX: Wait for the browser to actually stabilize before sending CDP commands
        time.sleep(3) 
        self.driver.implicitly_wait(5)

        try:
            # We wrap this in a try-except because UC sometimes handles this 
            # natively, but we want to be sure for headless mode.
            self.driver.execute_cdp_cmd(
                "Page.setDownloadBehavior",
                {"behavior": "allow", "downloadPath": abs_download_dir},
            )
            logging.info("CDP download behavior set successfully.")
        except Exception as e:
            logging.warning(f"Could not set CDP download behavior (may not be required): {e}")

    def _handle_captcha(self):
        """Checks for captcha and waits for human solution if detected."""
        captcha_keywords = ["captcha", "g-recaptcha", "cf-challenge", "h-captcha", "verify you are human"]
        
        try:
            page_source = self.driver.page_source.lower()
            if any(k in page_source for k in captcha_keywords):
                logging.warning(f"CAPTCHA detected! Please solve it in the browser. Waiting up to {self.CAPTCHA_WAIT_TIME} seconds...")
                
                start_wait = time.time()
                while time.time() - start_wait < self.CAPTCHA_WAIT_TIME:
                    time.sleep(5)
                    # Check if the keywords are gone from page source
                    try:
                        current_source = self.driver.page_source.lower()
                        if not any(k in current_source for k in captcha_keywords):
                            logging.info("CAPTCHA cleared! Resuming download...")
                            return True
                    except:
                        break # Browser might have moved on
                
                logging.error("Manual CAPTCHA solve timeout reached.")
                return False
            return True
        except Exception as e:
            logging.error(f"Error checking for CAPTCHA: {e}")
            return True

    def _wait_for_pdf_load(self, timeout: int = 30) -> bool:
        try:
            wait = WebDriverWait(self.driver, timeout)

            try:
                logging.info("Checking for PDF embed element...")
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "embed")))
                embed = self.driver.find_element(By.TAG_NAME, "embed")
                embed_type = embed.get_attribute("type")

                if "pdf" in embed_type.lower():
                    logging.info(f"PDF embed found with type: {embed_type}")
                    return True
            except:
                logging.info("No embed element found, checking alternative methods...")

            current_url = self.driver.current_url
            if current_url.lower().endswith(".pdf") or "pdf" in current_url.lower():
                logging.info(f"PDF detected in URL: {current_url}")
                time.sleep(3)
                return True

            page_title = self.driver.title
            if ".pdf" in page_title.lower() or "pdf" in page_title.lower():
                logging.info(f"PDF detected in page title: {page_title}")
                time.sleep(3)
                return True

            logging.warning("Could not confirm PDF is loaded")
            return False

        except Exception as e:
            logging.error(f"Error waiting for PDF to load: {str(e)}")
            return False

    def default_pdf_downloader(self, url: str) -> bool:
        try:
            logging.info(f"\n{'='*60}")
            logging.info(f"Starting PDF download from: {url}")
            logging.info(f"{'='*60}")

            self._random_delay()

            logging.info(f"Navigating to: {url}")
            self.driver.get(url)

            # Check for captcha immediately after navigation
            if not self._handle_captcha():
                return False

            time.sleep(self.PAGE_LOAD_WAIT)

            logging.info("Checking for automatic download...")
            download_success = self._check_download_status(
                timeout=10, check_interval=0.5
            )

            if not download_success:
                logging.info(
                    "No automatic download detected, triggering manual download (Ctrl+S)..."
                )

                self._random_delay(1.0, 2.0)

                actions = ActionChains(self.driver)
                actions.send_keys(Keys.CONTROL + "s").perform()

                logging.info("Download command sent, checking download status...")
                download_success = self._check_download_status(
                    timeout=60, check_interval=1.0
                )

            if download_success:
                logging.info(f"Download successful from: {url}")
                return True
            else:
                logging.error(f"Download failed from: {url}")
                return False

        except Exception as e:
            logging.error(f"Error downloading PDF from {url}: {str(e)}")
            import traceback

            logging.error(traceback.format_exc())
            return False

    def download_all(self) -> tuple[dict, dict]:
        results = {"successful": 0, "failed": 0, "total": len(self.urls)}
        url_to_filename = {}

        logging.info(f"\n{'='*60}")
        logging.info(f"Starting batch download of {results['total']} URLs")
        logging.info(
            f"Human-like behavior: {'ENABLED' if self.random_delay else 'DISABLED'}"
        )
        logging.info(f"{'='*60}\n")

        for idx, url in enumerate(self.urls, 1):
            logging.info(f"Processing URL {idx}/{results['total']}")

            files_before = self.downloaded_files.copy()

            try:
                success = self.default_pdf_downloader(url)
            except Exception as e:
                logging.error(f"Exception during download: {str(e)}")
                success = False

            if success:
                results["successful"] += 1

                new_files = self.downloaded_files - files_before
                if new_files:
                    new_filename = list(new_files)[0]
                    url_to_filename[url] = new_filename
                    logging.info(f"Mapped: {url} -> {new_filename}")
                else:
                    url_to_filename[url] = ""
                    logging.warning(
                        f"Download successful but no new file detected for: {url}"
                    )
            else:
                results["failed"] += 1
                url_to_filename[url] = ""

            if idx < results["total"]:
                delay = random.uniform(self.MIN_PAGE_DELAY, self.MAX_PAGE_DELAY)
                logging.info(f"Waiting {delay:.2f}s before next download...")
                time.sleep(delay)

        logging.info(f"\n{'='*60}")
        logging.info(f"Download Summary:")
        logging.info(f"  Total URLs: {results['total']}")
        logging.info(f"  Successful: {results['successful']}")
        logging.info(f"  Failed: {results['failed']}")
        logging.info(
            f"  Success Rate: {results['successful']/results['total']*100:.1f}%"
        )
        logging.info(f"{'='*60}\n")

        return results, url_to_filename

    def cleanup(self):
        if self.driver:
            logging.info("Closing browser...")
            self.driver.quit()
            logging.info("Browser closed")
