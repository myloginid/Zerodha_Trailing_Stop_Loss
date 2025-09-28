"""
Selenium-based web login helper for Kite Connect.

Encapsulates browser setup, login, 2FA (TOTP), and request_token capture,
so the main module stays lean.
"""

import os
import time
import stat
import re
from urllib.parse import urlparse, parse_qs
import random

import pyotp
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


class KiteWebLogin:
    def __init__(self, chromedriver_path: str | None = None, headless: bool = True):
        self.chromedriver_path = chromedriver_path or ""
        self.headless = headless
        self.driver = None

    def _init_webdriver(self):
        """Initialize Chrome WebDriver with appropriate options and ensure executable permissions"""
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        if self.headless:
            try:
                chrome_options.add_argument('--headless=new')
            except Exception:
                chrome_options.add_argument('--headless')

        driver_path = None

        # Prefer explicit path if provided and executable
        if self.chromedriver_path and os.path.exists(self.chromedriver_path) and os.access(self.chromedriver_path, os.X_OK):
            driver_path = self.chromedriver_path
            print(f"Using provided chromedriver_path: {driver_path}")
        else:
            try:
                driver_path = ChromeDriverManager().install()
                print(f"Initial driver path from webdriver-manager: {driver_path}")

                if driver_path and ('THIRD_PARTY' in driver_path or not driver_path.endswith('chromedriver')):
                    print("Detected incorrect driver path, searching for actual chromedriver...")
                    import glob
                    driver_dir = os.path.dirname(driver_path)
                    parent_dir = os.path.dirname(driver_dir)
                    search_dirs = [driver_dir, parent_dir]
                    found_driver = None
                    for search_dir in search_dirs:
                        if os.path.exists(search_dir):
                            patterns = [
                                os.path.join(search_dir, 'chromedriver'),
                                os.path.join(search_dir, 'chromedriver.exe'),
                                os.path.join(search_dir, '**/chromedriver'),
                                os.path.join(search_dir, '**/chromedriver.exe'),
                            ]
                            for pattern in patterns:
                                matches = glob.glob(pattern, recursive=True)
                                for match in matches:
                                    if (
                                        os.path.exists(match)
                                        and os.path.isfile(match)
                                        and 'THIRD_PARTY' not in match
                                        and 'LICENSE' not in match
                                    ):
                                        print(f"Found potential chromedriver at: {match}")
                                        found_driver = match
                                        break
                                if found_driver:
                                    break
                        if found_driver:
                            break
                    if found_driver:
                        driver_path = found_driver
                        print(f"Using corrected driver path: {driver_path}")
                    else:
                        print("Could not find valid chromedriver, will try system fallback")
                        driver_path = None

                # Ensure the downloaded/found chromedriver is executable
                if driver_path and os.path.exists(driver_path) and os.path.isfile(driver_path):
                    st = os.stat(driver_path)
                    os.chmod(driver_path, st.st_mode | stat.S_IEXEC)
                    print(f"Set executable permissions for: {driver_path}")
                else:
                    print(f"Warning: Chromedriver not found at {driver_path} after installation attempt.")
                    driver_path = None

            except Exception as e:
                print(f"Error with webdriver_manager: {str(e)}. Attempting fallback to system path.")
                driver_path = None

            if driver_path is None:
                # Fallback to common system paths for chromedriver
                common_paths = [
                    '/usr/bin/chromedriver',
                    '/usr/local/bin/chromedriver',
                    '/opt/google/chrome/chromedriver'
                ]
                for path in common_paths:
                    if os.path.exists(path) and os.access(path, os.X_OK):
                        driver_path = path
                        print(f"Using system chromedriver from: {driver_path}")
                        break

        if driver_path is None:
            raise Exception(
                "Chromedriver not found or not executable. "
                "Please ensure Chrome is installed and a compatible Chromedriver is available "
                "in PATH or pass chromedriver_path via app config."
            )

        service = Service(driver_path)
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        time.sleep(random.uniform(0.5, 1.2))

    def _perform_login(self, kite, user_id: str, password: str):
        try:
            login_url = kite.login_url()
            self.driver.get(login_url)
            time.sleep(random.uniform(0.8, 1.8))
            wait = WebDriverWait(self.driver, 10)
            userid_field = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input#userid")))
            userid_field.clear()
            userid_field.send_keys(user_id)
            time.sleep(random.uniform(0.3, 0.8))
            password_field = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input#password")))
            password_field.clear()
            password_field.send_keys(password)
            time.sleep(random.uniform(0.3, 0.8))
            login_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']")))
            login_button.click()
            time.sleep(random.uniform(0.8, 1.6))
        except Exception as e:
            raise Exception(f"Login failed: {str(e)}")

    def _handle_2fa(self, totp_secret: str):
        try:
            wait = WebDriverWait(self.driver, 15)
            print("Waiting for 2FA page to load...")
            totp_field = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input#userid")))
            print("2FA field found, generating TOTP code...")
            totp = pyotp.TOTP(totp_secret)
            totp_code = totp.now()
            print(f"Generated TOTP code: {totp_code}")
            print(f"Time remaining for this code: {30 - (int(time.time()) % 30)} seconds")
            totp_field.clear()
            time.sleep(random.uniform(0.3, 0.7))
            totp_field.send_keys(totp_code)
            print("TOTP code entered, waiting for submission...")
            try:
                submit_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit'], .button-orange")))
                submit_button.click()
                print("Submit button clicked")
                time.sleep(random.uniform(0.8, 1.6))
            except Exception:
                print("No submit button found, waiting for automatic submission...")
                time.sleep(random.uniform(1.5, 3.0))
            time.sleep(random.uniform(1.5, 3.0))
            print(f"Current URL after 2FA attempt: {self.driver.current_url}")
        except Exception as e:
            raise Exception(f"2FA authentication failed: {str(e)}")

    def _handle_consent_if_present(self):
        """Some flows present an 'Allow/Continue' consent screen. Try to click it."""
        try:
            wait = WebDriverWait(self.driver, 5)
            # Try a few common button texts/selectors
            candidates = [
                (
                    By.XPATH,
                    "//button[contains(translate(., 'ALLOW', 'allow'),'allow') "
                    "or contains(translate(., 'CONTINUE','continue'),'continue')]",
                ),
                (By.CSS_SELECTOR, "button.button-orange"),
                (By.CSS_SELECTOR, "button[type='submit']"),
                (By.XPATH, "//a[contains(@href,'request_token=')]")
            ]
            for by, sel in candidates:
                try:
                    el = wait.until(EC.element_to_be_clickable((by, sel)))
                    el.click()
                    time.sleep(random.uniform(0.6, 1.2))
                    break
                except Exception:
                    continue
        except Exception:
            pass

    def _capture_request_token(self) -> str:
        try:
            # Try consent click just in case
            self._handle_consent_if_present()

            print("Waiting for redirect or token after 2FA...")
            deadline = time.time() + 60
            last_url = None
            token_re = re.compile(r"request_token=([A-Za-z0-9]{20,40})")
            while time.time() < deadline:
                current_url = self.driver.current_url
                if current_url != last_url:
                    print(f"Current URL: {current_url}")
                    last_url = current_url
                # Check URL query/fragment first
                parsed_url = urlparse(current_url)
                qp = parse_qs(parsed_url.query)
                if 'request_token' in qp:
                    return qp['request_token'][0]
                if parsed_url.fragment and 'request_token=' in parsed_url.fragment:
                    fp = parse_qs(parsed_url.fragment)
                    if 'request_token' in fp:
                        return fp['request_token'][0]

                # Check anchors on the page
                try:
                    links = self.driver.find_elements(By.TAG_NAME, 'a')
                    for a in links:
                        href = a.get_attribute('href') or ''
                        m = token_re.search(href)
                        if m:
                            return m.group(1)
                except Exception:
                    pass

                # Check page source/text for token pattern
                try:
                    html = self.driver.page_source or ''
                    m = token_re.search(html)
                    if m:
                        return m.group(1)
                    body_text = self.driver.execute_script("return document.body ? document.body.innerText : '';") or ''
                    m = token_re.search(body_text)
                    if m:
                        return m.group(1)
                except Exception:
                    pass

                time.sleep(1)

            # Final log
            current_url = self.driver.current_url
            print(f"Final URL without request_token: {current_url}")
            raise ValueError("Request token not found after waiting")
        except Exception as e:
            print(f"Error details - Current URL: {self.driver.current_url if self.driver else 'No driver'}")
            raise Exception(f"Failed to capture request token: {str(e)}")

    def login_and_get_request_token(self, kite, user_id: str, password: str, totp_secret: str) -> str:
        try:
            self._init_webdriver()
            print("Performing initial login...")
            self._perform_login(kite, user_id, password)
            print("Handling two-factor authentication...")
            self._handle_2fa(totp_secret)
            print("Capturing request token...")
            request_token = self._capture_request_token()
            print("Request token captured successfully")
            return request_token
        finally:
            if self.driver:
                self.driver.quit()
                print("\nBrowser closed.")
