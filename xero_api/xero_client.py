import os
import json
import pickle
from datetime import datetime, timedelta
from xero_python.api_client import ApiClient, Configuration
from xero_python.api_client.oauth2 import OAuth2Token
from xero_python.accounting import AccountingApi
from xero_python.payrollau import PayrollAuApi
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import TokenExpiredError

TOKEN_FILE = 'xero_token.json'  # Using JSON for better readability and consistency
TOKEN_EXPIRY_BUFFER = 300  # 5 minutes buffer

class XeroClient:
    def __init__(self):
        self.client_id = os.getenv('CLIENT_ID')
        self.client_secret = os.getenv('CLIENT_SECRET')
        self.redirect_uri = os.getenv('REDIRECT_URI')
        self.token = self._load_token()
        
        # Initialize APIs with token
        self.accounting_api = None
        self.payroll_api = None
        self._init_apis()
    
    def _init_apis(self):
        """Initialize API clients with current token"""
        if not self.token:
            print("No token available. Please authenticate first.")
            return
            
        try:
            # Create a session with the token
            session = OAuth2Session(
                self.client_id,
                token=self.token,
                auto_refresh_kwargs={
                    'client_id': self.client_id,
                    'client_secret': self.client_secret
                },
                auto_refresh_url='https://identity.xero.com/connect/token',
                token_updater=self._save_token
            )
            
            # Configure the API client with OAuth2 token
            config = Configuration()
            config.access_token = session.token['access_token']
            config.verify_ssl = True
            
            # Initialize API client
            api_client = ApiClient(config)
            
            # Initialize APIs
            self.accounting_api = AccountingApi(api_client)
            self.payroll_api = PayrollAuApi(api_client)
            
            # Get the first organization to set tenant_id
            orgs = self.accounting_api.get_organisations()
            if orgs and orgs.organisations:
                self.tenant_id = orgs.organisations[0].tenant_id
                print(f"Using organization: {orgs.organisations[0].name}")
            else:
                raise Exception("No organizations found for this account")
            
        except Exception as e:
            print(f"Error initializing APIs: {e}")
            raise

    def _configure_client(self):
        """Configure API client with current token"""
        if not self.token:
            raise ValueError("No valid token available. Please authenticate first.")
            
        # Create a session with the token
        session = OAuth2Session(
            self.client_id,
            token=self.token,
            auto_refresh_kwargs={
                'client_id': self.client_id,
                'client_secret': self.client_secret
            },
            auto_refresh_url='https://identity.xero.com/connect/token',
            token_updater=self._save_token
        )
        
        # Configure the API client with OAuth2 token
        config = Configuration()
        config.access_token = session.token['access_token']
        config.verify_ssl = True
        return config

    def _load_token(self):
        if os.path.exists(TOKEN_FILE):
            try:
                with open(TOKEN_FILE, 'r') as f:
                    token_data = json.load(f)
                    print("Token loaded successfully")
                    # Ensure scope is a string
                    if isinstance(token_data.get('scope'), list):
                        token_data['scope'] = ' '.join(token_data['scope'])
                    return token_data
            except (json.JSONDecodeError, Exception) as e:
                print(f"Error loading token file: {e}")
                return None
        print("No token file found")
        return None

    def _save_token(self, token):
        with open(TOKEN_FILE, 'w') as f:
            json.dump(token, f)

    def is_token_valid(self):
        if not self.token:
            return False
        expires_at = datetime.fromtimestamp(self.token.get('expires_at', 0))
        return expires_at > datetime.now() + timedelta(seconds=TOKEN_EXPIRY_BUFFER)

    def refresh_token(self):
        try:
            extra = {
                'client_id': self.client_id,
                'client_secret': self.client_secret
            }
            
            # Create a new session to refresh the token
            session = OAuth2Session(
                self.client_id,
                token=self.token,
                auto_refresh_kwargs=extra,
                auto_refresh_url='https://identity.xero.com/connect/token',
                token_updater=self._save_token
            )
            
            # Trigger a refresh
            session.get('https://api.xero.com/connections')
            return True
            
        except TokenExpiredError:
            print("Token expired. Please re-authenticate.")
            return False
        except Exception as e:
            print(f"Error refreshing token: {e}")
            return False

    def ensure_valid_token(self):
        """Ensure we have a valid token, refresh if needed"""
        if not self.token:
            print("No token found. Please run get_token_new.py first.")
            return False
            
        try:
            # If we don't have APIs initialized, initialize them
            if not hasattr(self, 'accounting_api') or not self.accounting_api:
                self._init_apis()
                return True
                
            # If token is expired, try to refresh it
            if not self.is_token_valid():
                print("Token expired or invalid, attempting to refresh...")
                if not self.refresh_token():
                    print("Failed to refresh token. Please re-authenticate.")
                    return False
                    
                # Re-initialize APIs with new token
                self._init_apis()
            
            return True
            
        except Exception as e:
            print(f"Token validation failed: {e}")
            return False
