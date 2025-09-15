import os
import json
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
import threading

# Load environment variables
load_dotenv()

# Configuration
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI', 'http://localhost:5000/callback')
TOKEN_FILE = 'xero_token.json'
EXPORT_FOLDER = 'xero_exports'

# Base URLs for Xero API
BASE_URLS = {
    'accounting': 'https://api.xero.com/api.xro/2.0',
    'finance': 'https://api.xero.com/finance.xro/1.0'
}

class XeroAuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        
        query = urlparse(self.path).query
        params = parse_qs(query)
        
        if 'code' in params:
            code = params['code'][0]
            self.server.auth_code = code
            self.wfile.write(b"<h1>Authentication successful! You can close this window.</h1>")
        else:
            self.wfile.write(b"<h1>Authentication failed. Please try again.</h1>")
        
        self.server.shutdown()

def get_auth_url():
    """Generate the authorization URL for Xero OAuth2"""
    auth_url = (
        f"https://login.xero.com/identity/connect/authorize?"
        f"response_type=code&"
        f"client_id={CLIENT_ID}&"
        f"redirect_uri={REDIRECT_URI}&"
        "scope=accounting.transactions%20accounting.contacts%20accounting.reports.read%20offline_access&"
        "state=123"
    )
    return auth_url

def get_token_from_code(auth_code):
    """Exchange authorization code for access token"""
    token_url = 'https://identity.xero.com/connect/token'
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Basic {get_basic_token()}'
    }
    
    data = {
        'grant_type': 'authorization_code',
        'code': auth_code,
        'redirect_uri': REDIRECT_URI
    }
    
    response = requests.post(token_url, headers=headers, data=data)
    response.raise_for_status()
    
    token_data = response.json()
    token_data['expires_at'] = datetime.now().timestamp() + token_data['expires_in']
    
    with open(TOKEN_FILE, 'w') as f:
        json.dump(token_data, f)
    
    return token_data

def get_basic_token():
    """Generate Basic Auth token for Xero API"""
    import base64
    return base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()

def get_headers():
    """Get headers with current access token"""
    token_data = get_token()
    return {
        'Authorization': f'Bearer {token_data["access_token"]}',
        'Xero-tenant-id': token_data.get('tenant_id', ''),
        'Accept': 'application/json'
    }

def get_token():
    """Get current token or refresh if expired"""
    if not os.path.exists(TOKEN_FILE):
        raise Exception("No token found. Please run the authentication process first.")
    
    with open(TOKEN_FILE, 'r') as f:
        token_data = json.load(f)
    
    if datetime.now().timestamp() >= token_data['expires_at'] - 60:  # Refresh 1 minute before expiry
        token_data = refresh_token(token_data)
    
    return token_data

def refresh_token(token_data):
    """Refresh the access token using refresh token"""
    token_url = 'https://identity.xero.com/connect/token'
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Basic {get_basic_token()}'
    }
    
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': token_data['refresh_token']
    }
    
    response = requests.post(token_url, headers=headers, data=data)
    response.raise_for_status()
    
    new_token_data = response.json()
    new_token_data['expires_at'] = datetime.now().timestamp() + new_token_data['expires_in']
    
    # Preserve the refresh token if not returned
    if 'refresh_token' not in new_token_data:
        new_token_data['refresh_token'] = token_data['refresh_token']
    
    with open(TOKEN_FILE, 'w') as f:
        json.dump(new_token_data, f)
    
    return new_token_data

def get_tenant_id():
    """Get the first tenant ID from the token"""
    token_data = get_token()
    
    if 'tenant_id' in token_data:
        return token_data['tenant_id']
    
    # If tenant_id is not in token, fetch it from the connections endpoint
    connections_url = 'https://api.xero.com/connections'
    headers = get_headers()
    
    response = requests.get(connections_url, headers=headers)
    response.raise_for_status()
    
    connections = response.json()
    if not connections:
        raise Exception("No tenants found for this account")
    
    # Use the first tenant by default
    tenant_id = connections[0]['tenantId']
    
    # Save tenant_id to token file
    token_data['tenant_id'] = tenant_id
    with open(TOKEN_FILE, 'w') as f:
        json.dump(token_data, f)
    
    return tenant_id

def make_api_call(endpoint, params=None):
    """Make API call to Xero"""
    url = f"{BASE_URLS['accounting']}/{endpoint}"
    headers = get_headers()
    
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    
    return response.json()

def export_to_csv(data, filename):
    """Export data to CSV file"""
    os.makedirs(EXPORT_FOLDER, exist_ok=True)
    filepath = os.path.join(EXPORT_FOLDER, f"{filename}.csv")
    
    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict) and 'Rows' in data:
        # Handle Xero report format
        rows = []
        for row in data.get('Rows', []):
            if 'Rows' in row:  # Section
                rows.extend(row['Rows'])
            else:  # Row
                rows.append(row)
        df = pd.DataFrame(rows)
    else:
        df = pd.DataFrame([data])
    
    df.to_csv(filepath, index=False)
    print(f"Exported {len(df)} rows to {filepath}")
    return filepath

def get_report(report_name, params=None):
    """Get Xero report"""
    endpoint = f"Reports/{report_name}"
    return make_api_call(endpoint, params)

def export_all_reports():
    """Export all available reports"""
    reports = [
        ('ProfitAndLoss', 'profit_and_loss'),
        ('BalanceSheet', 'balance_sheet'),
        ('AgedReceivablesByContact', 'aged_receivables'),
        ('AgedPayablesByContact', 'aged_payables'),
        ('BankSummary', 'bank_summary'),
        ('ExecutiveSummary', 'executive_summary')
    ]
    
    exported_files = []
    for report_id, report_name in reports:
        try:
            print(f"Exporting {report_name}...")
            data = get_report(report_id)
            filepath = export_to_csv(data, report_name)
            exported_files.append(filepath)
        except Exception as e:
            print(f"Error exporting {report_name}: {str(e)}")
    
    return exported_files

def export_contacts():
    """Export contacts to CSV"""
    print("Exporting contacts...")
    data = make_api_call('Contacts')
    return export_to_csv(data.get('Contacts', []), 'contacts')

def export_invoices():
    """Export invoices to CSV"""
    print("Exporting invoices...")
    data = make_api_call('Invoices')
    return export_to_csv(data.get('Invoices', []), 'invoices')

def export_bank_transactions():
    """Export bank transactions to CSV"""
    print("Exporting bank transactions...")
    data = make_api_call('BankTransactions')
    return export_to_csv(data.get('BankTransactions', []), 'bank_transactions')

def authenticate():
    """Handle OAuth2 authentication flow"""
    if os.path.exists(TOKEN_FILE):
        try:
            # Test if token is valid
            get_tenant_id()
            print("Already authenticated with Xero")
            return
        except Exception as e:
            print(f"Authentication error: {str(e)}")
            print("Re-authenticating...")
    
    # Start local server for OAuth callback
    server_address = ('', 5000)
    httpd = HTTPServer(server_address, XeroAuthHandler)
    
    # Open browser for authentication
    auth_url = get_auth_url()
    print(f"Please visit this URL to authorize the application: {auth_url}")
    webbrowser.open(auth_url)
    
    # Wait for the callback
    print("Waiting for authentication...")
    httpd.handle_request()
    
    if hasattr(httpd, 'auth_code'):
        print("Authentication successful! Getting access token...")
        get_token_from_code(httpd.auth_code)
        print("Successfully authenticated with Xero")
    else:
        print("Authentication failed. Please try again.")
        exit(1)

def main():
    """Main function to run the export"""
    try:
        # Ensure we're authenticated
        authenticate()
        
        # Get and set tenant ID
        tenant_id = get_tenant_id()
        print(f"Using tenant ID: {tenant_id}")
        
        # Export all data
        print("\nStarting data export...")
        export_contacts()
        export_invoices()
        export_bank_transactions()
        export_all_reports()
        
        print("\n✅ Export completed successfully!")
        print(f"📁 Check the '{EXPORT_FOLDER}' directory for exported files.")
        
    except Exception as e:
        print(f"\n❌ An error occurred: {str(e)}")
        print("\nPlease make sure you have the correct permissions and try again.")
        if os.path.exists(TOKEN_FILE):
            print("You may need to delete the token file and re-authenticate.")

if __name__ == "__main__":
    main()
