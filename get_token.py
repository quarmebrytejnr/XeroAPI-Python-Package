import os
import webbrowser
import json
import time
import datetime
from wsgiref.simple_server import make_server
from urllib.parse import urlencode, parse_qs
from dotenv import load_dotenv

from flask import Flask, request, redirect, jsonify
from xero_python.api_client import ApiClient
from xero_python.api_client.configuration import Configuration
from xero_python.api_client.oauth2 import OAuth2Token
from xero_python.identity import IdentityApi
import requests

from token_manager import save_xero_oauth2_token, get_xero_oauth2_token

# Load environment variables
load_dotenv()

# Get configuration from environment variables
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')

# Core scopes that are known to work
scopes = [
    "offline_access",  # Required for refresh tokens
    "openid",          # Required for OAuth2
    "profile",         # Basic profile information
    "email",           # Email address
    "accounting.transactions",  # Access to accounting data
    "accounting.contacts",      # Access to contacts
    "accounting.reports.read",  # Required for financial reports
    "accounting.settings"       # Required for some report configurations
]

# Uncomment and add finance.statements.read only after confirming basic auth works
# scopes.append("finance.statements.read")

auth_url = "https://login.xero.com/identity/connect/authorize"
token_url = "https://identity.xero.com/connect/token"

def get_auth_url():
    """Generate the authorization URL for Xero OAuth2."""
    auth_url = "https://login.xero.com/identity/connect/authorize"
    params = {
        'response_type': 'code',
        'client_id': CLIENT_ID,
        'redirect_uri': REDIRECT_URI,
        'scope': ' '.join(scopes),
        'state': '123'  # Optional but recommended for security
    }
    return f"{auth_url}?{urlencode(params)}"

def exchange_code_for_token(auth_code):
    """Exchange authorization code for access token."""
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    
    data = {
        'grant_type': 'authorization_code',
        'code': auth_code,
        'redirect_uri': REDIRECT_URI,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    }
    
    response = requests.post(token_url, headers=headers, data=data)
    response.raise_for_status()
    return response.json()

app = Flask(__name__)

@app.route('/')
def index():
    url = get_auth_url()
    webbrowser.open_new(url)
    return "Check your browser to authorize the app. If the browser doesn't open, visit this URL: <a href='{}'>{}</a>".format(url, url)

@app.route('/callback')
def callback():
    auth_code = request.args.get('code')
    if not auth_code:
        return "Error: No authorization code provided", 400
    
    try:
        # Exchange the authorization code for tokens
        token_response = exchange_code_for_token(auth_code)
        
        if not token_response or 'access_token' not in token_response:
            return "<h1>Error:</h1><p>Failed to obtain access token from Xero</p>", 400
        
        # Save the token with timestamp
        save_xero_oauth2_token(token_response)
        
        # Format token info for display
        expires_in = int(token_response.get('expires_in', 0))
        expires_at = token_response.get('stored_at', 0) + expires_in if 'stored_at' in token_response else 0
        expires_str = (datetime.datetime.fromtimestamp(expires_at).strftime('%Y-%m-%d %H:%M:%S') 
                      if expires_at else 'N/A')
        
        return f"""
        <h1>Success!</h1>
        <p>Authentication complete. You can now close this window and return to your application.</p>
        <div style="background: #f5f5f5; padding: 15px; border-radius: 5px; margin: 20px 0;">
            <h3>Token Information:</h3>
            <p><strong>Access Token:</strong> {token_response.get('access_token', 'N/A')[:30]}...</p>
            <p><strong>Refresh Token:</strong> {token_response.get('refresh_token', 'N/A')[:30]}...</p>
            <p><strong>Expires In:</strong> {expires_in} seconds</p>
            <p><strong>Expires At:</strong> {expires_str}</p>
            <p><strong>Token Type:</strong> {token_response.get('token_type', 'N/A')}</p>
        </div>
        <p>This information has been saved to xero_token.json</p>
        """
        
    except requests.exceptions.RequestException as e:
        return f"<h1>Error:</h1><p>{str(e)}</p><p>Response: {e.response.text if hasattr(e, 'response') else 'No response'}</p>", 400
    except Exception as e:
        return f"<h1>Error:</h1><p>{str(e)}</p>", 400

@app.route('/check_token')
def check_token():
    """Check if the current token is valid and when it expires."""
    token = get_xero_oauth2_token()
    if not token:
        return jsonify({"status": "error", "message": "No valid token found"}), 401
    
    expires_in = token.get('expires_at', 0) - int(time.time())
    return jsonify({
        "status": "valid" if expires_in > 0 else "expired",
        "expires_in_seconds": max(0, expires_in),
        "expires_at": token.get('expires_at'),
        "token_type": token.get('token_type')
    })

def main():
    # Check if we already have a valid token
    token = get_xero_oauth2_token()
    if token:
        expires_in = token.get('expires_at', 0) - int(time.time())
        if expires_in > 300:  # More than 5 minutes remaining
            print(f"Found valid token that expires in {expires_in} seconds")
            print("Starting server for token management...")
        else:
            print("Token expired or expiring soon. Starting OAuth flow...")
    else:
        print("No valid token found. Starting OAuth flow...")
    
    # Start the server
    print("Serving on http://localhost:5000")
    print("Open http://localhost:5000 in your browser to start the OAuth flow")
    app.run(host='localhost', port=5000, debug=True)

if __name__ == "__main__":
    main()