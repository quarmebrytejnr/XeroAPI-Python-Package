
import os
import json
import time
import requests
from datetime import datetime, timedelta
from xero_python.api_client.oauth2 import OAuth2Token
from dotenv import load_dotenv

load_dotenv()

TOKEN_FILE = "xero_token.json"
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

# Buffer time in seconds before token expiry to start refreshing (5 minutes)
TOKEN_REFRESH_BUFFER = 300

def store_token(token):
    # Add timestamp when token was stored
    token['stored_at'] = int(time.time())
    with open(TOKEN_FILE, "w") as f:
        json.dump(token, f, indent=2)

def load_token():
    if not os.path.exists(TOKEN_FILE):
        return None
    with open(TOKEN_FILE, "r") as f:
        return json.load(f)

def is_token_expired(token):
    """Check if token is expired or about to expire."""
    if not token or 'expires_in' not in token or 'stored_at' not in token:
        return True
        
    # Calculate when the token will expire
    expires_at = token['stored_at'] + token['expires_in']
    # Consider the token expired if we're within the refresh buffer
    return time.time() >= (expires_at - TOKEN_REFRESH_BUFFER)

def get_valid_token():
    """Get a valid token, refreshing if necessary."""
    token = load_token()
    
    if not token or 'refresh_token' not in token:
        return None
        
    if is_token_expired(token):
        try:
            print("Token expired or about to expire. Refreshing...")
            token = refresh_xero_oauth2_token(CLIENT_ID, CLIENT_SECRET, token)
            save_xero_oauth2_token(token)
            print("Token refreshed successfully")
        except Exception as e:
            print(f"Error refreshing token: {str(e)}")
            return None
    
    return token

def get_xero_oauth2_token():
    """Get a valid OAuth2 token, refreshing if necessary."""
    return get_valid_token()

def save_xero_oauth2_token(token):
    """Save token to file with timestamp."""
    store_token(token)

def refresh_xero_oauth2_token(client_id, client_secret, token):
    """Refreshes the Xero OAuth2 token."""
    if 'refresh_token' not in token:
        raise ValueError("Token dictionary must contain a 'refresh_token'")

    response = requests.post(
        "https://identity.xero.com/connect/token",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "refresh_token",
            "refresh_token": token['refresh_token'],
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    response.raise_for_status()
    new_token = response.json()

    # The new response might not include the refresh token if it's a long-lived one.
    # Preserve the old refresh token if a new one isn't provided.
    if 'refresh_token' not in new_token:
        new_token['refresh_token'] = token['refresh_token']
        
    # Add timestamp and calculate expiration
    current_time = int(time.time())
    new_token['stored_at'] = current_time
    new_token['expires_at'] = current_time + new_token.get('expires_in', 1800)  # Default to 30 mins if not provided
    
    return new_token
