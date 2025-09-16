import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
import base64

# Load environment variables
load_dotenv()

# Configuration
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI', 'http://localhost:5000/callback')
TOKEN_FILE = 'xero_token.json'
EXPORT_FOLDER = 'xero_exports'

# Base URLs for Xero API
BASE_URL = 'https://api.xero.com/api.xro/2.0'

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
    return (
        f"https://login.xero.com/identity/connect/authorize?"
        f"response_type=code&"
        f"client_id={CLIENT_ID}&"
        f"redirect_uri={REDIRECT_URI}&"
        "scope=accounting.transactions%20accounting.contacts%20accounting.reports.read%20offline_access&"
        "state=123"
    )

def get_token_from_code(auth_code):
    """Exchange authorization code for access token"""
    token_url = 'https://identity.xero.com/connect/token'
    auth_string = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Basic {auth_string}'
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

def get_headers():
    """Get headers with current access token"""
    if not os.path.exists(TOKEN_FILE):
        raise Exception("No token found. Please run the authentication process first.")
    
    with open(TOKEN_FILE, 'r') as f:
        token_data = json.load(f)
    
    # Refresh token if expired
    if datetime.now().timestamp() >= token_data['expires_at'] - 60:  # 60 seconds buffer
        token_url = 'https://identity.xero.com/connect/token'
        auth_string = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {auth_string}'
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
        
        token_data = new_token_data
    
    # Get tenant ID if not present
    if 'tenant_id' not in token_data:
        connections_url = 'https://api.xero.com/connections'
        headers = {
            'Authorization': f'Bearer {token_data["access_token"]}',
            'Accept': 'application/json'
        }
        
        response = requests.get(connections_url, headers=headers)
        response.raise_for_status()
        
        connections = response.json()
        if not connections:
            raise Exception("No tenants found for this account")
        
        # Use the first tenant by default
        token_data['tenant_id'] = connections[0]['tenantId']
        
        # Save tenant_id to token file
        with open(TOKEN_FILE, 'w') as f:
            json.dump(token_data, f)
    
    return {
        'Authorization': f'Bearer {token_data["access_token"]}',
        'Xero-tenant-id': token_data['tenant_id'],
        'Accept': 'application/json'
    }

def make_api_call(endpoint, params=None):
    """Make API call to Xero"""
    url = f"{BASE_URL}/{endpoint}"
    headers = get_headers()
    
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    
    return response.json()

def export_to_csv(data, filename):
    """Export data to CSV file with proper file handling"""
    try:
        os.makedirs(EXPORT_FOLDER, exist_ok=True)
        filepath = os.path.join(EXPORT_FOLDER, f"{filename}.csv")
        
        # Convert data to DataFrame
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
        
        # Write to CSV with explicit file handling
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            df.to_csv(f, index=False)
            
        print(f"✅ Exported {len(df)} rows to {filepath}")
        return filepath
        
    except Exception as e:
        print(f"❌ Error exporting {filename}: {str(e)}")
        return None

def export_report(report_name, params=None):
    """Export a single report"""
    try:
        print(f"Exporting {report_name}...")
        data = make_api_call(f"Reports/{report_name}", params)
        return export_to_csv(data, report_name.lower())
    except Exception as e:
        print(f"Error exporting {report_name}: {str(e)}")
        return None

def authenticate():
    """Handle OAuth2 authentication flow"""
    if os.path.exists(TOKEN_FILE):
        try:
            # Test if token is valid
            get_headers()
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

def export_all():
    """Export all data from Xero"""
    try:
        # Ensure we're authenticated
        authenticate()
        
        print("\nStarting data export...")
        
        # Export standard data
        data_sets = [
            ('Contacts', 'contacts'),
            ('Invoices', 'invoices'),
            ('BankTransactions', 'bank_transactions'),
            ('Accounts', 'chart_of_accounts')
            # ('Journals', 'journals'),  # Commented out as per user request
            # ('ManualJournals', 'manual_journals')  # Commented out as per user request
        ]
        
        for endpoint, filename in data_sets:
            try:
                print(f"Exporting {filename}...")
                data = make_api_call(endpoint)
                export_to_csv(data.get(endpoint, []), filename)
            except Exception as e:
                print(f"Error exporting {filename}: {str(e)}")
        
        # Export reports
        standard_reports = [
            'BalanceSheet',
            'AgedReceivablesByContact',
            'AgedPayablesByContact',
            'BankSummary',
            'ExecutiveSummary'
        ]
        
        # Export standard reports
        for report in standard_reports:
            try:
                export_report(report)
            except Exception as e:
                print(f"Error exporting {report}: {str(e)}")
        
        # Export Profit & Loss with daily breakdown
        print("\n=== Starting Daily P&L Export (Jan 1, 2025 to Today) ===")
        print("This will fetch each day's P&L report and combine them into one file...")
        
        try:
            # Initialize data structures
            all_rows = []
            processed_days = 0
            failed_days = 0
            
            # Set date range - from Jan 1, 2025 to today
            start_date = datetime(2025, 1, 1)
            end_date = datetime.now()
            
            # Calculate total days to process
            total_days = (end_date - start_date).days + 1
            
            print(f"\n📅 Exporting P&L from 2025-01-01 to {end_date.strftime('%Y-%m-%d')} ({total_days} days)")
            print("This may take several minutes. Please be patient...\n")
            
            # Process each day
            current_date = start_date
            day_count = 0
            
            while current_date <= end_date:
                day_count += 1
                date_str = current_date.strftime('%Y-%m-%d')
                
                print(f"\n📅 Day {day_count}/{total_days}: {date_str}")
                print("-" * 50)
                
                try:
                    # Configure API request with retry logic
                    max_retries = 3
                    retry_delay = 2  # seconds
                    data = None
                    
                    for attempt in range(max_retries):
                        try:
                            params = {
                                'fromDate': date_str,
                                'toDate': date_str,
                                'standardLayout': 'true',
                                'paymentsOnly': 'false',
                                'timeframe': 'DAY'
                            }
                            
                            print(f"  🔍 Fetching P&L data for {date_str} (Attempt {attempt + 1}/{max_retries})...")
                            data = make_api_call('Reports/ProfitAndLoss', params)
                            
                            if not data or 'Reports' not in data or not data['Reports']:
                                raise ValueError("Empty or invalid response from API")
                                
                            # Validate report data structure
                            report = data['Reports'][0]
                            if 'Rows' not in report or not report['Rows']:
                                raise ValueError("No rows found in report")
                                
                            break  # Success, exit retry loop
                            
                        except Exception as e:
                            if attempt == max_retries - 1:  # Last attempt
                                print(f"  ❌ Failed to fetch data for {date_str} after {max_retries} attempts")
                                print(f"  Error: {str(e)}")
                                failed_days += 1
                                raise  # Re-raise to be caught by outer exception handler
                            else:
                                print(f"  ⚠️ Attempt {attempt + 1} failed, retrying in {retry_delay} seconds...")
                                time.sleep(retry_delay)
                    
                    # We've already validated the report structure in the retry loop
                    report = data['Reports'][0]
                    report_date = current_date.strftime('%Y-%m-%d')
                    sections = report.get('Rows', [])
                    section_count = len(sections)
                    
                    if section_count == 0:
                        print(f"  ⚠️ No sections found in report for {date_str}")
                        failed_days += 1
                        current_date += timedelta(days=1)
                        continue
                        
                    print(f"  📊 Found {section_count} sections to process...")
                    
                    try:
                        
                        # Process each section in the report
                        for section in sections:
                            section_title = section.get('Title', 'No Section Title')
                            rows = section.get('Rows', [])
                            
                            for row in rows:
                                if 'Cells' not in row:
                                    continue
                                    
                                # Create a row with the report date and section info
                                row_data = {
                                    'ReportDate': report_date,
                                    'Section': section_title,
                                    'RowType': row.get('RowType', '')
                                }
                                
                                # Add cell data
                                for i, cell in enumerate(row['Cells']):
                                    col_name = f'Column_{i}' if i > 0 else 'Account'
                                    row_data[col_name] = cell.get('Value', '')
                                    
                                    # Add account ID if available
                                    if i == 0 and 'Attributes' in cell:
                                        attrs = cell.get('Attributes', [{}])
                                        if attrs and 'Value' in attrs[0]:
                                            row_data['AccountID'] = attrs[0]['Value']
                                
                                all_rows.append(row_data)
                        
                        print(f"  ✅ Added {len(rows)} rows from {date_str}")
                        
                    except Exception as e:
                        print(f"  ❌ Error processing report for {date_str}: {str(e)}")
                        if 'Reports' in data and data['Reports'] and len(data['Reports']) > 0:
                            print(f"  Report structure: {json.dumps(list(data['Reports'][0].keys()), indent=2)}")
                    
                    # Move to next day
                    current_date += timedelta(days=1)
                    
                except Exception as e:
                    print(f"⚠️ Error processing {date_str}: {str(e)}")
            
            # Calculate success rate
            processed_days = day_count - failed_days
            success_rate = (processed_days / day_count) * 100 if day_count > 0 else 0
            
            # Export all rows to CSV with detailed feedback
            if all_rows:
                output_file = export_to_csv(all_rows, 'profit_loss_daily_breakdown')
                if output_file:
                    print("\n" + "="*60)
                    print(f"✅ SUCCESS: Exported {len(all_rows)} rows from {processed_days} days to {output_file}")
                    print("="*60)
                    
                    # Show summary
                    print(f"\n📊 Export Summary:")
                    print(f"- Total days processed: {day_count}")
                    print(f"- Successfully exported: {processed_days} days")
                    print(f"- Failed days: {failed_days}")
                    print(f"- Success rate: {success_rate:.1f}%")
                    
                    # Show sample data
                    print("\nSample of exported data:")
                    sample = all_rows[:min(3, len(all_rows))]
                    for i, row in enumerate(sample, 1):
                        print(f"{i}. {row.get('Section', '')} - {row.get('Account', '')}: {row.get('Amount_1', 'N/A')}")
                else:
                    print("❌ Failed to export P&L data")
            else:
                print("\n⚠️ No P&L data found for the specified date range")
                    
        except Exception as e:
            print(f"\n❌ Error exporting P&L with daily breakdown: {str(e)}")
            if 'data' in locals():
                print("\nLast API Response:")
                print(json.dumps(data, indent=2)[:500] + "..." if data else "No data received")
        
        print("\n✅ Export process completed!")
        print(f"📁 Check the '{EXPORT_FOLDER}' directory for exported files.")
        
    except Exception as e:
        print(f"\n❌ A critical error occurred: {str(e)}")
        print("\nTroubleshooting steps:")
        print("1. Check your internet connection")
        print("2. Verify your Xero API credentials in the .env file")
        if os.path.exists(TOKEN_FILE):
            print("3. Try deleting the token file and re-authenticating")
        print("\nIf the problem persists, please contact support with the error details above.")

if __name__ == "__main__":
    export_all()
