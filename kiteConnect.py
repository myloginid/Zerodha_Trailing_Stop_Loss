"""
Kite Connect API Login Automation Module

Automates multi-account login for Zerodha Kite, persists daily holdings,
computes trailing-stop signals, and can email a daily HTML report.

Configuration is read from JSON files only (no environment variables):
- accounts.json: credentials per account
- app_config.json: app settings (driver path, headless, account selection, email config path, subject)
- email_config.json: SMTP credentials and recipients
"""

import os
import json
import time
import random
from datetime import datetime, date
 
from kiteconnect import KiteConnect

from web_login import KiteWebLogin

# ---- Holdings persistence using DuckDB-friendly files ----
from tsl import (duckdb_connect_with_holdings_view, compute_trailing_stop_signals, print_trailing_stop_summary)
from email_report import generate_daily_html_report, send_email_via_gmail
from data_pipeline import (
    ist_today_str,
    ist_last_business_date_str,
    compute_missing_maps,
    ensure_holdings_parquet_from_jsonl,
    ensure_funds_parquet_from_jsonl,
    funds_already_persisted,
    persist_holdings as dp_persist_holdings,
    persist_funds as dp_persist_funds,
    load_token_for_account as dp_load_token_for_account,
)


def load_app_config():
    """Load application settings from app_config.json next to this file.

    Returns a dict with keys (defaults in parentheses):
    - chromedriver_path ("")
    - chrome_headless (True)
    - selected_accounts ([] = all)
    - email_config_path ("email_config.json")
    - report_email_subject ("Daily Holdings & TSL - {date}")
    """
    base_dir = os.path.dirname(__file__)
    path = os.path.join(base_dir, 'app_config.json')
    cfg = {}
    if os.path.exists(path):
        try:
            with open(path, 'r') as f:
                cfg = json.load(f) or {}
        except Exception as e:
            print(f"Failed to read app config {path}: {e}")
            cfg = {}
    # Defaults (no env fallbacks)
    cfg.setdefault('chromedriver_path', '')
    cfg.setdefault('chrome_headless', True)
    cfg.setdefault('selected_accounts', [])
    cfg.setdefault('email_config_path', 'email_config.json')
    cfg.setdefault('report_email_subject', 'Daily Holdings & TSL - {date}')
    return cfg

def load_email_config(path: str | None = None):
    """Centralized loader for email config.

    Reads JSON from the provided path or default `email_config.json` next to this file.
    No environment variable fallbacks are used.
    """
    base_dir = os.path.dirname(__file__)
    path = path or os.path.join(base_dir, 'email_config.json')
    cfg = {}
    if os.path.exists(path):
        try:
            with open(path, 'r') as f:
                cfg = json.load(f) or {}
        except Exception as e:
            print(f"Failed to read email config {path}: {e}")
            cfg = {}
    # Defaults
    cfg.setdefault('SMTP_HOST', 'smtp.gmail.com')
    cfg.setdefault('SMTP_PORT', 587)
    cfg.setdefault('REPORT_EMAIL_TO', 'myloginid@gmail.com')
    return cfg

# Configuration Module
class Config:
    """Configuration loader supporting multiple Zerodha accounts.

    Precedence (per account):
    - Load accounts from `accounts.json` next to this file.
      Expected shape:
      {
        "accounts": {
          "acc1": {"api_key": "...", "api_secret": "...", "user_id": "...", "password": "...", "totp_secret": "..."},
          "acc2": { ... }
        }
      }
    - Else, fall back to the in-file defaults (legacy single-account usage).
    """

    # Paths and driver
    BASE_DIR = os.path.dirname(__file__)
    CONFIG_PATH = os.path.join(BASE_DIR, 'accounts.json')
    TOKENS_DIR = os.path.join(BASE_DIR, 'tokens')

    # Legacy single-account defaults (kept for backward compatibility)
    LEGACY_DEFAULTS = {
        'api_key': "g96j0z5c4iemz4w6",
        'api_secret': "w1e8qp8gppq4lvco6jziu6615cbwi5rx",
        'user_id': "RM4109",
        'password': "ZxcDsaQwe#21",
        'totp_secret': "2HTTP5V4MYFEA6DICKODQVCPZYTFSB7S",
    }

    _accounts_cache = None

    @classmethod
    def _load_from_file(cls):
        if os.path.exists(cls.CONFIG_PATH):
            try:
                with open(cls.CONFIG_PATH, 'r') as f:
                    data = json.load(f)
                accounts = data.get('accounts') or {}
                # Normalize keys to a consistent schema
                norm = {}
                for name, acc in accounts.items():
                    norm[name] = {
                        'api_key': acc.get('api_key'),
                        'api_secret': acc.get('api_secret'),
                        'user_id': acc.get('user_id'),
                        'password': acc.get('password'),
                        'totp_secret': acc.get('totp_secret') or acc.get('totp_secret_key'),
                    }
                return norm
            except Exception as e:
                raise ValueError(f"Failed to read accounts from {cls.CONFIG_PATH}: {e}")
        return None

    @classmethod
    def _load_from_env(cls):
        """Environment loading disabled: use accounts.json only."""
        return None

    @classmethod
    def load_accounts(cls):
        if cls._accounts_cache is not None:
            return cls._accounts_cache

        accounts = cls._load_from_file()
        if not accounts:
            # Fall back to legacy defaults under a 'default' profile
            accounts = {'default': cls.LEGACY_DEFAULTS.copy()}

        # Basic validation
        for name, acc in accounts.items():
            missing = [k for k in ['api_key', 'api_secret', 'user_id', 'password', 'totp_secret'] if not acc.get(k)]
            if missing:
                raise ValueError(f"Account '{name}' missing required fields: {', '.join(missing)}")

        cls._accounts_cache = accounts
        return accounts

    @classmethod
    def get_account(cls, name='default'):
        accounts = cls.load_accounts()
        if name not in accounts:
            raise KeyError(f"Account '{name}' not found in configuration.")
        return accounts[name]

class KiteLoginAutomation:
    """Handles automated login process for Kite Connect"""
    
    def __init__(self, account_name: str = 'default'):
        """Initialize login automation for a given account profile.

        Args:
            account_name: Name of the account profile from the config.
        """
        self.account_name = account_name
        acc = Config.get_account(account_name)
        self.api_key = acc['api_key']
        self.api_secret = acc['api_secret']
        self.user_id = acc['user_id']
        self.password = acc['password']
        self.totp_secret = acc['totp_secret']
        self.kite = KiteConnect(api_key=self.api_key)
    def _generate_access_token(self, request_token):
        """Generate access token using request token"""
        try:
            # Generate session to get access token
            data = self.kite.generate_session(request_token, api_secret=self.api_secret)
            
            if 'access_token' not in data:
                raise ValueError("Access token not found in session data")
            
            access_token = data['access_token']
            return access_token
            
        except Exception as e:
            raise Exception(f"Failed to generate access token: {str(e)}")
    
    def _token_paths(self):
        base = Config.TOKENS_DIR
        os.makedirs(base, exist_ok=True)
        json_path = os.path.join(base, f"{self.account_name}_access_token.json")
        txt_path = os.path.join(base, f"{self.account_name}_access_token.txt")
        return json_path, txt_path

    def _load_existing_token(self):
        """Load existing access token if it exists and is valid for today"""
        try:
            token_file_json, _ = self._token_paths()
            if os.path.exists(token_file_json):
                with open(token_file_json, 'r') as f:
                    token_data = json.load(f)
                
                # Check if token is for today
                token_date = datetime.strptime(token_data.get('date', ''), '%Y-%m-%d').date()
                today = date.today()
                
                if token_date == today:
                    print(f"Found valid access token for today ({today}) for account '{self.account_name}'")
                    return token_data.get('access_token')
                else:
                    print(f"Existing token is from {token_date}, need new token for {today}")
                    return None
            else:
                print(f"No existing token file found for account '{self.account_name}'")
                return None
        except Exception as e:
            print(f"Error loading existing token: {str(e)}")
            return None
    
    def _save_access_token(self, access_token):
        """Save access token to file with current date"""
        try:
            token_data = {
                'access_token': access_token,
                'date': date.today().strftime('%Y-%m-%d'),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            json_path, txt_path = self._token_paths()
            # Save as JSON with date
            with open(json_path, 'w') as f:
                json.dump(token_data, f, indent=2)
            
            # Also save as plain text for backward compatibility
            with open(txt_path, 'w') as f:
                f.write(access_token)
            
            print(f"\nAccess token saved with date: {token_data['date']} for account '{self.account_name}'")
            print(f"Files: {json_path}, {txt_path}")
        except Exception as e:
            print(f"Warning: Could not save access token to file: {str(e)}")
    
    def login(self):
        """Main method to perform complete login process"""
        try:
            # Check for existing valid token first
            existing_token = self._load_existing_token()
            if existing_token:
                print("Using existing access token for today")
                return existing_token
            
            print("No valid token found, proceeding with login...")
            # Delegate Selenium flow to helper with app_config settings
            app_cfg = load_app_config()
            web = KiteWebLogin(
                chromedriver_path=app_cfg.get('chromedriver_path', ''),
                headless=bool(app_cfg.get('chrome_headless', True)),
            )
            request_token = web.login_and_get_request_token(
                kite=self.kite,
                user_id=self.user_id,
                password=self.password,
                totp_secret=self.totp_secret,
            )
            
            # Step 4: Generate access token
            print("Generating access token...")
            access_token = self._generate_access_token(request_token)
            print(f"Access token generated successfully")
            
            # Step 5: Save access token
            self._save_access_token(access_token)
            
            return access_token
            
        except Exception as e:
            print(f"\nLogin process failed: {str(e)}")
            raise
            
        finally:
            pass

class KiteTrader:
    """Primary interface for all trading-related activities using KiteConnect API.

    Note: Most methods are not invoked by this module's `main()` flow; they are
    exposed for external scripts to reuse. Keep signatures stable.
    """
    
    def __init__(self, api_key, access_token):
        """
        Initialize KiteTrader with API credentials
        
        Args:
            api_key (str): Kite Connect API key
            access_token (str): Valid access token obtained from login
        """
        self.api_key = api_key
        self.access_token = access_token
        self.kite = KiteConnect(api_key=api_key)
        self.kite.set_access_token(access_token)
        
    # Market Data Methods
    
    def get_instruments(self, exchange=None):
        """
        Fetches and returns the full list of tradable instruments
        
        Args:
            exchange (str, optional): Exchange code (e.g., 'NSE', 'NFO', 'BSE', 'MCX')
        
        Returns:
            list: List of instrument dictionaries containing tradingsymbol, exchange_token, etc.
        """
        try:
            if exchange:
                instruments = self.kite.instruments(exchange)
            else:
                instruments = self.kite.instruments()
            return instruments
        except Exception as e:
            print(f"Error fetching instruments: {str(e)}")
            return []
    
    def get_quote(self, instruments):
        """
        Retrieves full quote data including market depth for given instruments
        
        Args:
            instruments (list): List of instrument identifiers in format EXCHANGE:TRADINGSYMBOL
        
        Returns:
            dict: Dictionary with instrument identifiers as keys and quote data as values
        """
        try:
            quotes = self.kite.quote(instruments)
            return quotes
        except Exception as e:
            print(f"Error fetching quotes: {str(e)}")
            return {}
    
    def get_ltp(self, instruments):
        """
        Retrieves last traded price for given instruments
        
        Args:
            instruments (list): List of instrument identifiers in format EXCHANGE:TRADINGSYMBOL
        
        Returns:
            dict: Dictionary with instrument identifiers as keys and LTP data as values
        """
        try:
            ltp_data = self.kite.ltp(instruments)
            return ltp_data
        except Exception as e:
            print(f"Error fetching LTP: {str(e)}")
            return {}
    
    def get_historical_data(self, instrument_token, from_date, to_date, interval):
        """
        Fetches historical OHLC data for an instrument
        
        Args:
            instrument_token (int): Instrument token
            from_date (datetime): Start date for historical data
            to_date (datetime): End date for historical data
            interval (str): Candle interval (minute, day, 3minute, 5minute, 10minute, 15minute, 30minute, 60minute)
        
        Returns:
            list: List of historical data candles with date, open, high, low, close, volume
        """
        try:
            historical_data = self.kite.historical_data(
                instrument_token=instrument_token,
                from_date=from_date,
                to_date=to_date,
                interval=interval
            )
            return historical_data
        except Exception as e:
            print(f"Error fetching historical data: {str(e)}")
            return []
    
    # Order Management Methods
    
    def place_limit_order(self, exchange, tradingsymbol, transaction_type, quantity, price):
        """
        Places a regular limit order
        
        Args:
            exchange (str): Exchange (NSE, BSE, NFO, MCX, etc.)
            tradingsymbol (str): Trading symbol of the instrument
            transaction_type (str): BUY or SELL
            quantity (int): Quantity to trade
            price (float): Limit price
        
        Returns:
            dict: Order response containing order_id
        """
        try:
            order_id = self.kite.place_order(
                variety="regular",
                exchange=exchange,
                tradingsymbol=tradingsymbol,
                transaction_type=transaction_type,
                quantity=quantity,
                product="MIS",
                order_type="LIMIT",
                price=price,
                validity="DAY"
            )
            print(f"Limit order placed successfully. Order ID: {order_id}")
            print(f"  {transaction_type} {quantity} shares of {tradingsymbol} at ₹{price:.2f}")
            return {"order_id": order_id, "status": "success"}
        except Exception as e:
            print(f"Error placing limit order: {str(e)}")
            return {"order_id": None, "status": "failed", "error": str(e)}
    
    def place_order(self, variety, exchange, tradingsymbol, transaction_type, 
                   quantity, product, order_type, price=None, trigger_price=None,
                   validity=None, tag=None):
        """
        Places an order with specified parameters
        
        Args:
            variety (str): Order variety (regular, bo, co, amo)
            exchange (str): Exchange (NSE, BSE, NFO, MCX, etc.)
            tradingsymbol (str): Trading symbol of the instrument
            transaction_type (str): BUY or SELL
            quantity (int): Quantity to trade
            product (str): Product type (CNC, NRML, MIS)
            order_type (str): Order type (MARKET, LIMIT, SL, SL-M)
            price (float, optional): Order price for LIMIT orders
            trigger_price (float, optional): Trigger price for SL orders
            validity (str, optional): Order validity (DAY, IOC, TTL)
            tag (str, optional): Optional order tag for tracking
        
        Returns:
            dict: Order response containing order_id
        """
        try:
            order_id = self.kite.place_order(
                variety=variety,
                exchange=exchange,
                tradingsymbol=tradingsymbol,
                transaction_type=transaction_type,
                quantity=quantity,
                product=product,
                order_type=order_type,
                price=price,
                trigger_price=trigger_price,
                validity=validity,
                tag=tag
            )
            print(f"Order placed successfully. Order ID: {order_id}")
            return {"order_id": order_id, "status": "success"}
        except Exception as e:
            print(f"Error placing order: {str(e)}")
            return {"order_id": None, "status": "failed", "error": str(e)}
    
    def place_bracket_order(self, exchange, tradingsymbol, transaction_type,
                           quantity, price, stoploss, target, trailing_stoploss=None):
        """
        Places a Bracket Order (BO) with profit target and stoploss
        
        Args:
            exchange (str): Exchange (NSE, BSE, NFO, MCX, etc.)
            tradingsymbol (str): Trading symbol of the instrument
            transaction_type (str): BUY or SELL
            quantity (int): Quantity to trade
            price (float): Entry price
            stoploss (float): Absolute stoploss value
            target (float): Absolute target value
            trailing_stoploss (float, optional): Trailing stoploss value
        
        Returns:
            dict: Order response containing order_id
        """
        try:
            # Calculate stoploss and target prices relative to entry price
            if transaction_type == "BUY":
                stoploss_price = price - abs(price - stoploss)
                target_price = price + abs(target - price)
            else:  # SELL
                stoploss_price = price + abs(price - stoploss)
                target_price = price - abs(target - price)
            
            order_id = self.kite.place_order(
                variety="bo",  # Use string instead of constant
                exchange=exchange,
                tradingsymbol=tradingsymbol,
                transaction_type=transaction_type,
                quantity=quantity,
                product="MIS",  # Use string instead of constant
                order_type="LIMIT",  # Use string instead of constant
                price=price,
                stoploss=stoploss_price,
                squareoff=target_price,
                trailing_stoploss=trailing_stoploss
            )
            print(f"Bracket order placed successfully. Order ID: {order_id}")
            return {"order_id": order_id, "status": "success"}
        except Exception as e:
            print(f"Error placing bracket order: {str(e)}")
            print(f"Note: Bracket orders may not be available. Falling back to regular order with manual stop-loss management.")
            # Fallback to regular order
            return self._place_regular_order_with_manual_sl(
                exchange, tradingsymbol, transaction_type, quantity, price, stoploss, target
            )
    
    def place_cover_order(self, exchange, tradingsymbol, transaction_type,
                         quantity, price, trigger_price):
        """
        Places a Cover Order (CO) with a compulsory stop loss
        
        Args:
            exchange (str): Exchange (NSE, BSE, NFO, MCX, etc.)
            tradingsymbol (str): Trading symbol of the instrument
            transaction_type (str): BUY or SELL
            quantity (int): Quantity to trade
            price (float): Limit price (0 for market order)
            trigger_price (float): Stop loss trigger price
        
        Returns:
            dict: Order response containing order_id
        """
        try:
            order_type = "MARKET" if price == 0 else "LIMIT"
            order_id = self.kite.place_order(
                variety="co",  # Use string instead of constant
                exchange=exchange,
                tradingsymbol=tradingsymbol,
                transaction_type=transaction_type,
                quantity=quantity,
                product="MIS",  # Use string instead of constant
                order_type=order_type,
                price=price if price != 0 else None,
                trigger_price=trigger_price
            )
            print(f"Cover order placed successfully. Order ID: {order_id}")
            return {"order_id": order_id, "status": "success"}
        except Exception as e:
            print(f"Error placing cover order: {str(e)}")
            return {"order_id": None, "status": "failed", "error": str(e)}
    
    def modify_order(self, variety, order_id, parent_order_id=None, quantity=None,
                    price=None, order_type=None, trigger_price=None, validity=None):
        """
        Modifies a pending order
        
        Args:
            variety (str): Order variety (regular, bo, co, amo)
            order_id (str): Order ID to modify
            parent_order_id (str, optional): Parent order ID for BO/CO orders
            quantity (int, optional): New quantity
            price (float, optional): New price
            order_type (str, optional): New order type
            trigger_price (float, optional): New trigger price
            validity (str, optional): New validity
        
        Returns:
            dict: Modification response
        """
        try:
            self.kite.modify_order(
                variety=variety,
                order_id=order_id,
                parent_order_id=parent_order_id,
                quantity=quantity,
                price=price,
                order_type=order_type,
                trigger_price=trigger_price,
                validity=validity
            )
            print(f"Order {order_id} modified successfully")
            return {"status": "success", "order_id": order_id}
        except Exception as e:
            print(f"Error modifying order: {str(e)}")
            return {"status": "failed", "error": str(e)}
    
    def cancel_order(self, variety, order_id, parent_order_id=None):
        """
        Cancels a pending order
        
        Args:
            variety (str): Order variety (regular, bo, co, amo)
            order_id (str): Order ID to cancel
            parent_order_id (str, optional): Parent order ID for BO/CO orders
        
        Returns:
            dict: Cancellation response
        """
        try:
            self.kite.cancel_order(
                variety=variety,
                order_id=order_id,
                parent_order_id=parent_order_id
            )
            print(f"Order {order_id} cancelled successfully")
            return {"status": "success", "order_id": order_id}
        except Exception as e:
            print(f"Error cancelling order: {str(e)}")
            return {"status": "failed", "error": str(e)}
    
    # Position & Fund Management Methods
    
    def get_positions(self):
        """
        Retrieves net positions for the day
        
        Returns:
            dict: Dictionary containing 'day' and 'net' positions
        """
        try:
            positions = self.kite.positions()
            return positions
        except Exception as e:
            print(f"Error fetching positions: {str(e)}")
            return {"day": [], "net": []}
    
    def get_holdings(self):
        """
        Retrieves user's holdings (long-term positions)
        
        Returns:
            list: List of holding dictionaries containing quantity, average_price, etc.
        """
        try:
            holdings = self.kite.holdings()
            return holdings
        except Exception as e:
            print(f"Error fetching holdings: {str(e)}")
            return []
    
    def get_margins(self):
        """
        Retrieves available margins in equity and commodity segments
        
        Returns:
            dict: Dictionary containing equity and commodity margin details
        """
        try:
            margins = self.kite.margins()
            return margins
        except Exception as e:
            print(f"Error fetching margins: {str(e)}")
            return {}

def login_all_accounts(names=None):
    """Login all configured Zerodha accounts and return KiteTrader instances.

    Args:
        names (list[str] | None): Optional subset of account names to login. If None,
            uses all accounts from the current configuration.

    Returns:
        dict: Mapping of account name -> KiteTrader instance (logged in with today's token).

    Notes:
        - Reuses saved tokens under `tokens/<name>_access_token.json` if valid for today.
        - Continues on errors for individual accounts and prints the exception.
    """
    traders = {}
    all_accounts = Config.load_accounts()
    target_names = names or list(all_accounts.keys())

    for idx, name in enumerate(target_names):
        try:
            print(f"\n--- Logging in account: {name} ---")
            automation = KiteLoginAutomation(account_name=name)
            token = automation.login()
            traders[name] = KiteTrader(api_key=automation.api_key, access_token=token)
            print(f"Account '{name}' ready.")
        except Exception as e:
            print(f"Failed to initialize account '{name}': {e}")
        # Small random delay between accounts to avoid server bursts
        if idx < len(target_names) - 1:
            time.sleep(random.uniform(1.5, 3.5))

    return traders

def main():
    """Main function to execute Kite Connect login automation"""
    print("Kite Connect Login Automation")
    print("="*50)
    
    try:
        # Determine which accounts to use from app_config.json (empty = all)
        all_accounts = Config.load_accounts()
        app_cfg = load_app_config()
        selected = app_cfg.get('selected_accounts') or []
        target_names = [x for x in selected if x in all_accounts] or list(all_accounts.keys())

        # Decide target snapshot date: today on weekdays, Friday on weekends
        ist_today = ist_today_str()
        snapshot_date = ist_last_business_date_str()

        print(f"Using accounts: {', '.join(target_names)}")
        print(f"Target snapshot date (IST): {snapshot_date}")

        # Determine missing per account
        base_dir = os.path.dirname(__file__)
        missing_holdings, missing_funds = compute_missing_maps(base_dir, target_names, snapshot_date)

        # Decide which accounts must login (missing holdings OR missing funds with no valid token)
        acceptable_token_dates = [snapshot_date, ist_today]
        needs_login = []
        for n in target_names:
            if missing_holdings[n]:
                needs_login.append(n)
                continue
            if missing_funds[n] and not dp_load_token_for_account(Config.TOKENS_DIR, n, acceptable_token_dates):
                needs_login.append(n)

        if needs_login:
            print("\n" + "="*50)
            print(f"Logging in for: {', '.join(needs_login)}")
            traders = login_all_accounts(names=needs_login)
            if not traders:
                print("No accounts logged in for data fetch. Check configuration.")
            else:
                print(f"Logged in accounts: {', '.join(traders.keys())}")
                for name, t in traders.items():
                    # Fetch only the missing parts
                    if missing_holdings.get(name):
                        try:
                            h = t.get_holdings()
                            print(f"\n[{name}] Holdings count: {len(h)}")
                            # Persist holdings
                            paths = dp_persist_holdings(base_dir, name, h, snapshot_date)
                            print(f"[{name}] Saved JSONL: {paths['jsonl']}")
                            if paths['parquet']:
                                print(f"[{name}] Saved Parquet: {paths['parquet']}")
                        except Exception as e:
                            print(f"[{name}] Failed to fetch holdings: {e}")
                    if missing_funds.get(name):
                        try:
                            funds = t.get_margins()
                            fpaths = dp_persist_funds(base_dir, name, funds, snapshot_date)
                            print(f"[{name}] Saved Funds JSONL: {fpaths['jsonl']}")
                        except Exception as e:
                            print(f"[{name}] Failed to fetch funds: {e}")
        else:
            print("Data fetch skipped (already persisted for target date or tokens available).")
        # Ensure Parquet exists for all target accounts for the snapshot date
        for name in target_names:
            ensure_holdings_parquet_from_jsonl(base_dir, name, snapshot_date)
            ensure_funds_parquet_from_jsonl(base_dir, name, snapshot_date)

        # Fetch funds for accounts not fetched above (no extra login; requires valid token)
        for name in target_names:
            if funds_already_persisted(base_dir, name, snapshot_date):
                continue
            # Try to reuse today's or snapshot_date token from file
            token = dp_load_token_for_account(Config.TOKENS_DIR, name, acceptable_token_dates)
            if not token:
                print(f"[{name}] Skipping funds fetch (no valid token for {snapshot_date}).")
                continue
            try:
                acc = Config.get_account(name)
                temp_trader = KiteTrader(api_key=acc['api_key'], access_token=token)
                funds = temp_trader.get_margins()
                fpaths = dp_persist_funds(base_dir, name, funds, snapshot_date)
                print(f"[{name}] Saved Funds JSONL: {fpaths['jsonl']}")
            except Exception as e:
                print(f"[{name}] Failed to fetch funds with token: {e}")

        # If DuckDB is available and parquet exists, compute trailing stop signals
        try:
            con = duckdb_connect_with_holdings_view()
            if con is not None:
                signals = compute_trailing_stop_signals(con, for_date=snapshot_date)
                print_trailing_stop_summary(signals)
                # Build and send HTML report
                html = generate_daily_html_report(con, signals, for_date=snapshot_date)
                email_cfg_path = os.path.join(os.path.dirname(__file__), app_cfg.get('email_config_path', 'email_config.json'))
                email_cfg = load_email_config(path=email_cfg_path)
                to_list = [
                    addr.strip()
                    for addr in str(email_cfg.get('REPORT_EMAIL_TO', 'myloginid@gmail.com')).split(',')
                    if addr.strip()
                ]
                subject_tmpl = app_cfg.get('report_email_subject', 'Daily Holdings & TSL - {date}')
                subject = subject_tmpl.format(date=signals.get('as_of_date'))
                send_email_via_gmail(
                    subject,
                    html,
                    to_list,
                    smtp_user=email_cfg.get('SMTP_USER'),
                    smtp_pass=email_cfg.get('SMTP_PASS'),
                    smtp_from=email_cfg.get('SMTP_FROM') or email_cfg.get('SMTP_USER'),
                    smtp_host=email_cfg.get('SMTP_HOST', 'smtp.gmail.com'),
                    smtp_port=int(email_cfg.get('SMTP_PORT', 587)),
                )
                con.close()
            else:
                print("DuckDB not available; skipping trailing stop analysis.")
        except Exception as e:
            print(f"Trailing stop analysis failed: {e}")

        return None
        
    except Exception as e:
        print(f"\nError: {str(e)}")
        return None

if __name__ == "__main__":
    main()
