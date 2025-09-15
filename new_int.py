import os
import requests
import pandas as pd
import json
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')
import time
from dotenv import load_dotenv
from token_manager import get_xero_oauth2_token

# Load environment variables
load_dotenv()

# Get tenant ID from environment variables
TENANT_ID = os.getenv('TENANT_ID')
if not TENANT_ID:
    raise ValueError("TENANT_ID environment variable is not set. Please add it to your .env file.")

# Base URLs for different Xero APIs
BASE_URLS = {
    'accounting': 'https://api.xero.com/api.xro/2.0',
    'finance': 'https://api.xero.com/finance.xro/1.0',  # Added for financial statements
    'payroll': 'https://api.xero.com/payroll.xro/1.0',
    'assets': 'https://api.xero.com/assets.xro/1.0',
    'files': 'https://api.xero.com/files.xro/1.0',
    'projects': 'https://api.xero.com/projects.xro/2.0'
}

# Common headers for all API calls
def get_headers():
    # Get a fresh token for each request
    token_data = get_xero_oauth2_token()
    if not token_data or 'access_token' not in token_data:
        raise Exception("No valid token available. Please run get_token.py first to authenticate.")
    
    return {
        'Authorization': f'Bearer {token_data["access_token"]}',
        'Xero-tenant-id': TENANT_ID,
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

def make_api_call(url, description="API Call"):
    """Make API call with error handling and return DataFrame when possible"""
    try:
        print(f"Fetching: {description}")
        response = requests.get(url, headers=get_headers())
        response.raise_for_status()
        
        data = response.json()
        print(f"Success: {description}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {description}: {str(e)}")
        return None

def get_tenant_id():
    """Get available tenant IDs - run this first if you don't have your tenant ID"""
    try:
        url = "https://api.xero.com/connections"
        headers = {
            'Authorization': f'Bearer {ACCESS_TOKEN}',
            'Accept': 'application/json'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        tenants = response.json()
        print("Available Tenants:")
        for tenant in tenants:
            print(f"ID: {tenant.get('tenantId')}, Name: {tenant.get('tenantName')}")
        return tenants
    except Exception as e:
        print(f"Error getting tenant ID: {str(e)}")
        if hasattr(e, 'response') and e.response:
            print(f"Response status: {e.response.status_code}")
            print(f"Response text: {e.response.text}")
        return None

def debug_token():
    """Debug token and connection issues"""
    print("🔍 Debugging Xero API Connection...")
    print(f"Token starts with: {ACCESS_TOKEN[:20]}...")
    print(f"Tenant ID: {TENANT_ID}")
    
    # Test connections endpoint first
    print("\n1. Testing connections endpoint...")
    try:
        url = "https://api.xero.com/connections"
        headers = {
            'Authorization': f'Bearer {ACCESS_TOKEN}',
            'Accept': 'application/json'
        }
        response = requests.get(url, headers=headers)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            tenants = response.json()
            print("✅ Token is valid!")
            print("Available Tenants:")
            for tenant in tenants:
                print(f"  - ID: {tenant.get('tenantId')}")
                print(f"    Name: {tenant.get('tenantName')}")
                print(f"    Type: {tenant.get('tenantType')}")
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
    
    # Test with tenant ID if provided
    if TENANT_ID != "your-tenant-id":
        print(f"\n2. Testing API call with tenant ID: {TENANT_ID}")
        try:
            url = f"{BASE_URLS['accounting']}/Organisation"
            response = requests.get(url, headers=get_headers())
            print(f"Status Code: {response.status_code}")
            
            if response.status_code == 200:
                print("✅ Tenant ID is valid!")
            else:
                print(f"❌ Tenant ID Error: {response.status_code}")
                print(f"Response: {response.text}")
                
        except Exception as e:
            print(f"❌ Exception: {str(e)}")
    else:
        print("\n⚠️ Please set your TENANT_ID first!")

def normalize_data(data, key_name, parent_id_name=None, parent_df=None):
    """
    Convert API response to a dictionary of DataFrames, expanding nested lists.
    """
    if not data:
        return {}

    # Find the list of items to process
    items = []
    if isinstance(data, dict):
        if key_name in data:
            items = data[key_name]
        elif 'Body' in data and key_name in data['Body']:
            items = data['Body'][key_name]
        else:
            for k, value in data.items():
                if isinstance(value, list) and len(value) > 0:
                    items = value
                    break
            else:
                items = [data]
    elif isinstance(data, list):
        items = data

    if not items:
        return {}

    try:
        # Define which nested fields to expand
        # Format: { 'field_to_expand': ('parent_id_field', 'new_table_suffix') }
        expand_rules = {
            'LineItems': ('InvoiceID', 'LineItems'),
            'JournalLines': ('JournalID', 'JournalLines'),
            'Payments': ('InvoiceID', 'Payments'),
            'CreditNotes': ('InvoiceID', 'CreditNotes'),
            'Addresses': ('ContactID', 'Addresses'),
            'Phones': ('ContactID', 'Phones'),
            'Rows': ('ReportID', 'Rows')
        }

        # Main DataFrame
        main_df = pd.json_normalize(items, record_path=None, meta_prefix='meta_')
        if main_df.empty:
            return {}
            
        main_df['LastUpdated'] = datetime.now()
        main_df = _process_dates_in_df(main_df)

        result_dfs = {key_name: main_df}

        # Expand nested columns
        for col, (parent_id, suffix) in expand_rules.items():
            if col in main_df.columns:
                # Create a unique ID for the parent if it doesn't exist
                if parent_id not in main_df.columns:
                    # Fallback to a generic ID if the specific one isn't present
                    parent_id_field = f"{key_name.lower()}ID"
                    if parent_id_field not in main_df.columns:
                         continue # Cannot link back, so skip expansion
                else:
                    parent_id_field = parent_id

                # Explode the nested list and normalize it
                expanded_df = main_df[[parent_id_field, col]].explode(col).dropna()
                if not expanded_df.empty:
                    normalized_child = pd.json_normalize(expanded_df[col])
                    
                    # Add the parent ID for relationship
                    normalized_child[parent_id_field] = expanded_df[parent_id_field].values
                    
                    # Clean up and add to results
                    normalized_child = _process_dates_in_df(normalized_child)
                    result_dfs[f"{key_name}_{suffix}"] = normalized_child

                # Drop the original complex column from the main df
                main_df.drop(columns=[col], inplace=True)
        
        # Convert any remaining complex columns to JSON strings as a fallback
        for col_name in main_df.select_dtypes(include=['object']).columns:
            if any(isinstance(i, (dict, list)) for i in main_df[col_name].dropna()):
                 main_df[col_name] = main_df[col_name].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)


        return result_dfs

    except Exception as e:
        import traceback
        print(f"Error normalizing {key_name}: {str(e)}")
        traceback.print_exc()
        return {}


def _process_dates_in_df(df: pd.DataFrame) -> pd.DataFrame:
    """Converts object columns that look like dates to datetime and formats them."""
    for col in df.columns:
        # Check if column is of object type (likely strings)
        if df[col].dtype == 'object':
            # Attempt to convert to datetime
            try:
                # Use errors='coerce' to turn unparseable dates into NaT (Not a Time)
                temp_series = pd.to_datetime(df[col], errors='coerce')
                
                # If a significant portion of the column was converted, assume it's a date column
                # and replace original with formatted strings
                if temp_series.count() / len(df) > 0.5: # More than 50% successfully converted
                    df[col] = temp_series.dt.strftime('%Y-%m-%d')
            except Exception:
                # If conversion fails, leave the column as is
                pass
    return df

START_DATE = "2025-01-01T00:00:00"
END_DATE = "2025-02-28T23:59:59"
DATE_FILTER = START_DATE  # For backward compatibility

def make_paginated_api_call(url, description="API Call", date_param="ModifiedAfter", key_name=None, params=None):
    all_items = []
    page = 1
    page_size = 100  # Default page size
    
    # Initialize params if not provided
    if params is None:
        params = {}
    
    # Add date range filter if supported and not already in params
    if date_param and date_param not in params:
        params[date_param] = START_DATE
    
    while True:
        try:
            # Prepare request parameters
            request_params = params.copy()
            request_params['page'] = page
            
            # Make the API call
            print(f"Fetching: {description} (Page {page})")
            response = requests.get(
                url,
                headers=get_headers(),
                params=request_params
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Handle different response formats
            if key_name and key_name in data:
                items = data[key_name]
            elif isinstance(data, list):
                items = data
            else:
                items = data.get('items', [])
            
            if not items:
                print(f"No more items found for {description}")
                break
                
            all_items.extend(items)
            print(f"  - Fetched {len(items)} items (Total: {len(all_items)})")
            
            # Check if we've reached the last page
            if len(items) < page_size:
                break
            
            # Move to next page
            page += 1
            
            # Be kind to the API - add a small delay between requests
            time.sleep(0.5)
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching {description} (Page {page}): {str(e)}")
            if hasattr(e, 'response') and e.response:
                print(f"  - Status: {e.response.status_code}")
                if e.response.text:
                    print(f"  - Response: {e.response.text[:500]}")
            
            # If it's a rate limit error, wait and retry
            if hasattr(e, 'response') and e.response.status_code == 429:
                retry_after = int(e.response.headers.get('Retry-After', 5))
                print(f"  ⏳ Rate limited. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                continue
                
            # For other errors, break the loop
            break
        except Exception as e:
            print(f"❌ Unexpected error: {str(e)}")
            import traceback
            traceback.print_exc()
            break
    
    print(f"✅ Fetched {len(all_items)} total items for {description}")
    return all_items

def make_api_call_with_date(url, description="API Call", date_param="ModifiedAfter"):
    """Make API call with date range filtering"""
    try:
        # Add date range filter if provided
        if date_param:
            # Add start date
            url = f"{url}{'&' if '?' in url else '?'}{date_param}={START_DATE}"
            # Add end date
            url = f"{url}&DateTo={END_DATE}"
        else:
            filtered_url = f"{url}?{date_param}={START_DATE}"
            
        response = requests.get(filtered_url, headers=get_headers())
        
        # If date filter fails, try without it
        if response.status_code == 400:
            print(f"Date filter not supported for {description}, getting all records...")
            response = requests.get(url, headers=get_headers())
            
        response.raise_for_status()
        
        data = response.json()
        print(f"✅ Success: {description}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching {description}: {str(e)}")
        return None

# =============================================================================
# MAIN DATA RETRIEVAL FUNCTIONS WITH DATE FILTERING
# =============================================================================

def get_credit_transactions():
    """Fetch all credit transactions including credit notes and bank transfers"""
    # Get credit notes
    credit_notes_url = f"{BASE_URLS['accounting']}/CreditNotes"
    credit_notes = make_api_call(credit_notes_url, "Credit Notes")
    
    # Get bank transactions (filter for credits)
    credit_types = "SPEND-OVERPAYMENT,SPEND-PREPAYMENT,SPEND-REFUND,RECEIVE-OVERPAYMENT,RECEIVE-PREPAYMENT,RECEIVE-REFUND"
    bank_transactions_url = f"{BASE_URLS['accounting']}/BankTransactions?where=Type==\"{credit_types}\""
    bank_transactions = make_api_call(bank_transactions_url, "Bank Credit Transactions")
    
    # Combine and process the data
    all_credits = []
    
    if credit_notes is not None and not credit_notes.empty:
        credit_notes['TransactionType'] = 'CreditNote'
        all_credits.append(credit_notes)
    
    if bank_transactions is not None and not bank_transactions.empty:
        bank_transactions['TransactionType'] = 'BankCredit'
        all_credits.append(bank_transactions)
    
    if all_credits:
        return pd.concat(all_credits, ignore_index=True)
    return pd.DataFrame()

def get_contacts():
    """Get all contacts (updated since Jan 1, 2025)"""
    data = make_paginated_api_call(f"{BASE_URLS['accounting']}/Contacts", "Contacts", key_name='Contacts')
    return normalize_data(data, 'Contacts')

def get_invoices(summary_only=False, page_size=100):
    """
    Get all invoices (since Jan 1, 2025) with full details including line items and payments
    
    Args:
        summary_only (bool): If True, returns a lightweight version without line items and payments
        page_size (int): Number of items to fetch per page (default: 100, max: 100)
        
    Returns:
        DataFrame: Pandas DataFrame containing invoice data with nested fields preserved as dictionaries/lists
    """
    import pandas as pd
    from tqdm import tqdm
    
    base_url = f"{BASE_URLS['accounting']}/Invoices"
    params = {
        'where': 'Date >= DateTime(2025, 1, 1)',
        'order': 'Date DESC',  # Get most recent invoices first
        'pageSize': min(int(page_size), 100)  # Ensure page size doesn't exceed 100
    }
    
    if not summary_only:
        params['includeOnline'] = 'true'  # Include all line items and payments
    else:
        params['summaryOnly'] = 'true'    # Lightweight version
    
    print(f"Fetching invoices (page size: {params['pageSize']})...")
    
    # First, get the total count to show progress
    try:
        test_params = params.copy()
        test_params['page'] = 1
        test_params['pageSize'] = 1
        
        response = requests.get(
            base_url,
            headers=get_headers(),
            params=test_params
        )
        response.raise_for_status()
        total_count = int(response.headers.get('x-total-count', 0))
    except Exception as e:
        print(f"⚠️ Could not get total count: {str(e)}")
        total_count = 0
    
    # Fetch all pages with progress bar
    all_invoices = []
    page = 1
    
    with tqdm(total=total_count or None, desc="Fetching invoices", unit="invoice") as pbar:
        while True:
            try:
                page_params = params.copy()
                page_params['page'] = page
                
                response = requests.get(
                    base_url,
                    headers=get_headers(),
                    params=page_params
                )
                response.raise_for_status()
                
                data = response.json()
                if not data or 'Invoices' not in data or not data['Invoices']:
                    break
                    
                all_invoices.extend(data['Invoices'])
                fetched = len(data['Invoices'])
                pbar.update(fetched)
                
                if fetched < params['pageSize']:
                    break  # Last page
                    
                page += 1
                
                # Be kind to the API
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                print(f"\n⚠️ Error fetching page {page}: {str(e)}")
                if hasattr(e, 'response') and e.response is not None:
                    print(f"Status code: {e.response.status_code}")
                    print(f"Response: {e.response.text[:500]}")
                break
            except Exception as e:
                print(f"\n⚠️ Unexpected error on page {page}: {str(e)}")
                break
    
    if not all_invoices:
        print("No invoice data returned from API")
        return pd.DataFrame()
    
    print(f"\nProcessing {len(all_invoices)} invoices...")
    
    # Convert to DataFrame with better memory management
    chunks = []
    chunk_size = 1000  # Process in chunks to manage memory
    
    for i in range(0, len(all_invoices), chunk_size):
        chunk = all_invoices[i:i + chunk_size]
        df_chunk = pd.json_normalize(chunk, sep='.', errors='ignore')
        chunks.append(df_chunk)
    
    # Combine chunks
    df = pd.concat(chunks, ignore_index=True)
    
    # Convert date fields with better error handling
    date_columns = ['Date', 'DueDate', 'FullyPaidOnDate', 'ExpectedPaymentDate', 
                   'PlannedPaymentDate', 'UpdatedDateUTC', 'DateString', 'DueDateString']
    
    for col in date_columns:
        if col in df.columns and not df[col].empty:
            try:
                # Skip if already datetime
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    continue
                    
                # Convert object/string columns
                if df[col].dtype == 'object':
                    # Handle Xero's /Date(123456789+0000)/ format
                    if df[col].astype(str).str.contains(r'^/Date\(\d+[+-]\d+\)/$', na=False).any():
                        # Extract timestamp part (milliseconds since epoch)
                        df[col] = pd.to_datetime(
                            df[col].str.extract(r'/Date\((\d+)', expand=False),
                            unit='ms',
                            errors='coerce'
                        )
                    # Handle ISO format strings
                    else:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception as e:
                print(f"⚠️ Error processing {col}: {str(e)}")
    
    # Convert numeric fields
    numeric_columns = ['SubTotal', 'TotalTax', 'Total', 'AmountDue', 'AmountPaid', 
                      'AmountCredited', 'CurrencyRate']
    
    for col in numeric_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except Exception as e:
                print(f"⚠️ Error converting {col} to numeric: {str(e)}")
    
    # Clean up memory
    del all_invoices
    
    print(f"✅ Successfully processed {len(df)} invoices")
    return df

def get_invoice_summaries():
    """Get lightweight invoice summaries (faster, without line items and payments)"""
    return get_invoices(summary_only=True)

def get_bills():
    """Get all bills (since Jan 1, 2025)"""
    data = make_paginated_api_call(f"{BASE_URLS['accounting']}/Invoices?Type=ACCPAY", "Bills", key_name='Invoices')
    return normalize_data(data, 'Invoices')

def get_manual_journals():
    """Get manual journals (since Jan 1, 2025)"""
    data = make_paginated_api_call(f"{BASE_URLS['accounting']}/ManualJournals", "Manual Journals", key_name='ManualJournals')
    return normalize_data(data, 'ManualJournals')

def get_journals():
    """Get journals (since Jan 1, 2025)"""
    data = make_paginated_api_call(f"{BASE_URLS['accounting']}/Journals", "Journals", key_name='Journals')
    return normalize_data(data, 'Journals')

def get_payments():
    """Get payments (since Jan 1, 2025)"""
    data = make_paginated_api_call(f"{BASE_URLS['accounting']}/Payments", "Payments", key_name='Payments')
    return normalize_data(data, 'Payments')

def get_receipts():
    """Get receipts (since Jan 1, 2025)"""
    data = make_paginated_api_call(f"{BASE_URLS['accounting']}/Receipts", "Receipts", key_name='Receipts')
    return normalize_data(data, 'Receipts')

def get_accounts():
    """Get chart of accounts"""
    data = make_paginated_api_call(f"{BASE_URLS['accounting']}/Accounts", "Chart of Accounts", key_name='Accounts', date_param=None)
    return normalize_data(data, 'Accounts')

def get_organisation():
    """Get organisation details"""
    data = make_paginated_api_call(f"{BASE_URLS['accounting']}/Organisation", "Organisation", key_name='Organisations', date_param=None)
    return normalize_data(data, 'Organisations')

def get_items():
    """Get inventory items"""
    data = make_paginated_api_call(f"{BASE_URLS['accounting']}/Items", "Items", key_name='Items', date_param=None)
    return normalize_data(data, 'Items')

def get_tax_rates():
    """Get tax rates"""
    data = make_paginated_api_call(f"{BASE_URLS['accounting']}/TaxRates", "Tax Rates", key_name='TaxRates', date_param=None)
    return normalize_data(data, 'TaxRates')

def get_currencies():
    """Get currencies"""
    data = make_paginated_api_call(f"{BASE_URLS['accounting']}/Currencies", "Currencies", key_name='Currencies', date_param=None)
    return normalize_data(data, 'Currencies')

def get_attachments():
    """Get attachments (since Jan 1, 2025)"""
    data = make_paginated_api_call(f"{BASE_URLS['accounting']}/Attachments", "Attachments", key_name='Attachments')
    return normalize_data(data, 'Attachments')

def get_budgets():
    """Get budgets"""
    data = make_api_call(f"{BASE_URLS['accounting']}/Budgets", "Budgets")
    return normalize_data(data, 'Budgets')

def get_aged_receivables_report():
    """Get Aged Receivables by Contact report"""
    url = f"{BASE_URLS['accounting']}/Reports/AgedReceivablesByContact"
    data = make_api_call_with_date(url, "Aged Receivables Report", date_param=None)
    return normalize_data(data, 'Reports')

def get_aged_payables_report():
    """Get Aged Payables by Contact report"""
    url = f"{BASE_URLS['accounting']}/Reports/AgedPayablesByContact"
    data = make_api_call_with_date(url, "Aged Payables Report", date_param=None)
    return normalize_data(data, 'Reports')

def get_bank_summary_report():
    """Get Bank Summary report"""
    url = f"{BASE_URLS['accounting']}/Reports/BankSummary"
    data = make_api_call_with_date(url, "Bank Summary Report", date_param=None)
    return normalize_data(data, 'Reports')

def get_executive_summary_report():
    """Get Executive Summary report"""
    url = f"{BASE_URLS['accounting']}/Reports/ExecutiveSummary"
    data = make_api_call_with_date(url, "Executive Summary Report", date_param=None)
    return normalize_data(data, 'Reports')

def get_profit_loss_report():
    """Get Profit & Loss report (for Jan-Feb 2025)"""
    url = f"{BASE_URLS['accounting']}/Reports/ProfitAndLoss?fromDate=2025-01-01&toDate=2025-02-28"
    data = make_api_call_with_date(url, "Profit & Loss Report", date_param=None)
    return normalize_data(data, 'Reports')

def get_balance_sheet_report():
    """Get Balance Sheet report (as of end of Feb 2025)"""
    data = make_api_call(f"{BASE_URLS['accounting']}/Reports/BalanceSheet?date=2025-02-28", "Balance Sheet Report")
    return normalize_data(data, 'Reports', 'ReportID')

def get_trial_balance_report():
    """Get Trial Balance report (as of end of Feb 2025)"""
    data = make_api_call(f"{BASE_URLS['accounting']}/Reports/TrialBalance?date=2025-02-28", "Trial Balance Report")
    return normalize_data(data, 'Reports', 'ReportID')

def get_profit_and_loss(start_date=None, end_date=None):
    """
    Get Profit & Loss statement from Xero Finance API
    
    Args:
        start_date (str): Start date in format 'YYYY-MM-DD'. Defaults to 12 months before end_date.
        end_date (str): End date in format 'YYYY-MM-DD'. Defaults to current date.
        
    Returns:
        dict: Profit and Loss statement data
        
    Raises:
        Exception: If there's an error fetching the data
    """
    try:
        params = {}
        if start_date:
            params['startDate'] = start_date
        if end_date:
            params['endDate'] = end_date
            
        url = f"{BASE_URLS['finance']}/financialstatements/profitandloss"
        print(f"Fetching P&L from {url} with params: {params}")
        
        headers = get_headers()
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=30
        )
        
        # Log response status and headers for debugging
        print(f"Response status: {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")
        
        response.raise_for_status()
        data = response.json()
        print(f"Successfully fetched P&L data")
        return data
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Error fetching P&L: {str(e)}"
        if hasattr(e, 'response') and e.response is not None:
            error_msg += f"\nStatus code: {e.response.status_code}"
            try:
                error_msg += f"\nResponse: {e.response.text[:500]}"
            except:
                pass
        print(error_msg)
        raise Exception(error_msg) from e

def get_balance_sheet(balance_date=None):
    """
    Get Balance Sheet from Xero Finance API
    
    Args:
        balance_date (str): Date in format 'YYYY-MM-DD'. Defaults to current date.
        
    Returns:
        dict: Balance Sheet data
        
    Raises:
        Exception: If there's an error fetching the data
    """
    try:
        params = {}
        if balance_date:
            params['balanceDate'] = balance_date
            
        url = f"{BASE_URLS['finance']}/financialstatements/balancesheet"
        print(f"Fetching Balance Sheet from {url} with params: {params}")
        
        headers = get_headers()
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=30
        )
        
        # Log response status and headers for debugging
        print(f"Response status: {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")
        
        response.raise_for_status()
        data = response.json()
        print("Successfully fetched Balance Sheet data")
        return data
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Error fetching Balance Sheet: {str(e)}"
        if hasattr(e, 'response') and e.response is not None:
            error_msg += f"\nStatus code: {e.response.status_code}"
            try:
                error_msg += f"\nResponse: {e.response.text[:500]}"
            except:
                pass
        print(error_msg)
        raise Exception(error_msg) from e

def get_cash_flow(start_date=None, end_date=None):
    """
    Get Cash Flow statement from Xero Finance API
    
    Args:
        start_date (str): Start date in format 'YYYY-MM-DD'. Defaults to 12 months before end_date.
        end_date (str): End date in format 'YYYY-MM-DD'. Defaults to current date.
        
    Returns:
        dict: Cash Flow statement data
        
    Raises:
        Exception: If there's an error fetching the data
    """
    try:
        params = {}
        if start_date:
            params['startDate'] = start_date
        if end_date:
            params['endDate'] = end_date
            
        url = f"{BASE_URLS['finance']}/financialstatements/cashflow"
        print(f"Fetching Cash Flow from {url} with params: {params}")
        
        headers = get_headers()
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=30
        )
        
        # Log response status and headers for debugging
        print(f"Response status: {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")
        
        response.raise_for_status()
        data = response.json()
        print("Successfully fetched Cash Flow data")
        return data
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Error fetching Cash Flow: {str(e)}"
        if hasattr(e, 'response') and e.response is not None:
            error_msg += f"\nStatus code: {e.response.status_code}"
            try:
                error_msg += f"\nResponse: {e.response.text[:500]}"
            except:
                pass
        print(error_msg)
        raise Exception(error_msg) from e

# PAYROLL FUNCTIONS
def get_employees():
    """Get payroll employees"""
    data = make_paginated_api_call(f"{BASE_URLS['payroll']}/Employees", "Employees", key_name='Employees', date_param=None)
    return normalize_data(data, 'Employees')

def get_pay_runs():
    """Get pay runs (since Jan 1, 2025)"""
    data = make_paginated_api_call(f"{BASE_URLS['payroll']}/PayRuns", "Pay Runs", key_name='PayRuns')
    return normalize_data(data, 'PayRuns')

def get_timesheets():
    """Get timesheets (since Jan 1, 2025)"""
    data = make_paginated_api_call(f"{BASE_URLS['payroll']}/Timesheets", "Timesheets", key_name='Timesheets')
    return normalize_data(data, 'Timesheets')

def get_payroll_settings():
    """Get payroll settings"""
    data = make_paginated_api_call(f"{BASE_URLS['payroll']}/Settings", "Payroll Settings", key_name='Settings', date_param=None)
    return normalize_data(data, 'Settings')

# OTHER FUNCTIONS
def get_assets():
    """Get fixed assets"""
    data = make_paginated_api_call(f"{BASE_URLS['assets']}/Assets", "Assets", key_name='Assets', date_param=None)
    return normalize_data(data, 'Assets')

def get_files():
    """Get files (since Jan 1, 2025)"""
    data = make_paginated_api_call(f"{BASE_URLS['files']}/Files", "Files", key_name='Files')
    return normalize_data(data, 'Files')

def get_folders():
    """Get folders"""
    data = make_paginated_api_call(f"{BASE_URLS['files']}/Folders", "Folders", key_name='Folders', date_param=None)
    return normalize_data(data, 'Folders')

def get_projects():
    """Get projects"""
    data = make_paginated_api_call(f"{BASE_URLS['projects']}/Projects", "Projects", key_name='Items', date_param=None)
    return normalize_data(data, 'Projects')

# =============================================================================
# CSV EXPORT FUNCTIONS
# =============================================================================

def save_to_csv(df, filename, folder="xero_data"):
    """Save DataFrame to CSV with proper handling"""
    import os
    
    # Create folder if it doesn't exist
    if not os.path.exists(folder):
        os.makedirs(folder)
    
    filepath = os.path.join(folder, f"{filename}.csv")
    
    if df is not None and not df.empty:
        try:
            df.to_csv(filepath, index=False, encoding='utf-8')
            print(f"Saved {len(df)} records to {filepath}")
            return filepath
        except Exception as e:
            print(f"Error saving {filename}: {str(e)}")
            return None
    else:
        print(f"No data to save for {filename}")
        return None

def export_super_invoice():
    """
    Export a comprehensive invoice report with all fields exactly as they appear in the API response
    """
    import pandas as pd
    import numpy as np
    from datetime import datetime
    
    print("\n=== Exporting Raw Invoice Data ===")
    
    def safe_convert(value):
        """Safely convert values to JSON-serializable types"""
        # Handle None values first
        if value is None:
            return None
            
        # Handle numpy arrays and pandas Series
        if hasattr(value, 'shape') or isinstance(value, (list, tuple, np.ndarray)):
            if hasattr(value, 'size') and value.size == 0:
                return None
            if hasattr(value, 'size') and value.size == 1:
                return safe_convert(value.item() if hasattr(value, 'item') else value[0])
            return [safe_convert(x) for x in value] if len(value) > 0 else None
            
        # Handle pandas NA/NaN values
        try:
            if pd.isna(value):
                return None
        except (ValueError, TypeError):
            pass
            
        # Handle datetime objects
        if hasattr(value, 'isoformat') and not isinstance(value, (str, bytes, bytearray)):
            return value.isoformat()
            
        # Handle dictionaries and pandas Series
        if isinstance(value, (dict, pd.Series)):
            return {str(k): safe_convert(v) for k, v in value.items()}
            
        # Handle basic types
        if isinstance(value, (int, float, str, bool)):
            return value
            
        # Convert any remaining types to string
        return str(value)
    
    try:
        # Create output directory if it doesn't exist
        output_dir = 'xero_exports'
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, 'xero_invoices_raw.csv')
        
        # Get all invoices with full details
        print("Fetching all invoices with full details...")
        invoices = get_invoices(summary_only=False)
        
        if invoices is None or invoices.empty:
            print("❌ No invoices found or error occurred")
            return None
        
        # Convert DataFrame to list of dicts, handling all data types
        all_invoices = []
        for _, row in invoices.iterrows():
            # Convert each row to a dictionary and process all values
            invoice_dict = {}
            for col in invoices.columns:
                value = row[col]
                invoice_dict[col] = safe_convert(value)
            all_invoices.append(invoice_dict)
        
        # Convert to DataFrame with proper handling of nested structures
        df = pd.json_normalize(all_invoices, sep='.')
        
        # Ensure all columns are strings to avoid serialization issues
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str)
        
        # Save to CSV with proper encoding
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"✅ Successfully exported {len(df)} invoices to {output_file}")
        return output_file
            
    except Exception as e:
        import traceback
        print(f"❌ Error exporting invoice data: {str(e)}")
        print("Stack trace:")
        traceback.print_exc()
        return None

def export_invoice_summaries():
    """Legacy function - now just calls export_super_invoice for backward compatibility"""
    print("Note: export_invoice_summaries() is deprecated. Using export_super_invoice() instead.")
    return export_super_invoice()

def export_all_to_csv():
    """Export all Xero data to separate CSV files"""
    print(f"Starting comprehensive Xero data export (since {DATE_FILTER})...")
    print(f"Date range: January 1, 2025 to {datetime.now().strftime('%B %d, %Y')}")
    
    exported_files = []
    
    # Define all functions to run
    all_functions = {
        "SUPER INVOICE REPORT": [
            (export_super_invoice, "Exporting comprehensive invoice report with line items...")
        ],
        "ACCOUNTING DATA": [
            (get_contacts, "contacts"),
            (get_invoices, "invoices"),
            (get_bills, "bills"),
            (get_manual_journals, "manual_journals"),
            (get_journals, "journals"),
            (get_payments, "payments"),
            (get_receipts, "receipts"),
            (get_accounts, "chart_of_accounts"),
            (get_items, "items"),
            (get_tax_rates, "tax_rates"),
            (get_currencies, "currencies"),
            (get_attachments, "attachments"),
            (get_budgets, "budgets"),
            (get_organisation, "organisation")
        ],
        "REPORTS": [
            (get_profit_loss_report, "profit_loss_report"),
            (get_balance_sheet_report, "balance_sheet_report"),
            (get_trial_balance_report, "trial_balance_report"),
            (get_aged_receivables_report, "aged_receivables_report"),
            (get_aged_payables_report, "aged_payables_report"),
            (get_bank_summary_report, "bank_summary_report"),
            (get_executive_summary_report, "executive_summary_report"),
        ],
        "PAYROLL DATA": [
            (get_employees, "employees"), (get_pay_runs, "pay_runs"),
            (get_timesheets, "timesheets"), (get_payroll_settings, "payroll_settings"),
        ],
        "OTHER DATA": [
            (get_assets, "assets"), (get_files, "files"),
            (get_folders, "folders"), (get_projects, "projects"),
        ]
    }

    # Track processed data to avoid duplicates
    processed_data = {}
    
    for category, functions in all_functions.items():
        print(f"\n=== {category} ===")
        for func, base_filename in functions:
            try:
                # Skip if we've already processed this function
                if func.__name__ in processed_data:
                    print(f"- Skipping duplicate function: {func.__name__}")
                    continue
                    
                # Mark this function as processed
                processed_data[func.__name__] = True
                
                # Call the function to get data
                result = func()
                
                # Handle different return types
                if result is None:
                    print(f"- No data returned for {base_filename}")
                    continue
                    
                # If result is a DataFrame, convert to dict with base_filename as key
                if isinstance(result, pd.DataFrame):
                    result = {base_filename: result}
                # If result is already a dict but empty
                elif isinstance(result, dict) and not result:
                    print(f"- Empty result for {base_filename}")
                    continue
                
                # Process the result dictionary
                if isinstance(result, dict):
                    for name, df in result.items():
                        if df is None or (hasattr(df, 'empty') and df.empty):
                            print(f"- No data in {name}")
                            continue
                            
                        # Generate a clean filename
                        if name == base_filename or not name:
                            filename = base_filename
                        else:
                            # Remove any duplicate base_filename from name
                            clean_name = name.replace(base_filename, '').strip('_')
                            filename = f"{base_filename}_{clean_name}" if clean_name else base_filename
                        
                        # Ensure filename is lowercase and clean
                        filename = filename.lower().replace(' ', '_')
                        
                        # Save to CSV
                        filepath = save_to_csv(df, filename)
                        if filepath:
                            exported_files.append(filepath)
                            print(f"- Exported: {filename} ({len(df)} rows)")

            except Exception as e:
                import traceback
                print(f"- Error processing {base_filename}: {str(e)}")
                traceback.print_exc()

    # SUMMARY
    print(f"\n=== EXPORT COMPLETE ===")
    print(f"Total files exported: {len(exported_files)}")
    print(f"Files saved in: ./xero_data/ folder")
    print(f"Data period: January 1, 2025 - {datetime.now().strftime('%B %d, %Y')}")
    
    if exported_files:
        print("\nExported files:")
        for filepath in sorted(exported_files):
            print(f"   - {filepath}")
    
    return exported_files


# =============================================================================
# POWER BI FRIENDLY FUNCTIONS (Updated)
# =============================================================================

def get_all_accounting_data():
    """Get all accounting data as a dictionary of DataFrames"""
    print("📊 Retrieving Accounting Data...")
    return {
        'Contacts': get_contacts(),
        'Invoices': get_invoices(),
        'Bills': get_bills(),
        'CreditNotes': get_credit_notes(),
        'BankTransactions': get_bank_transactions(),
        'ManualJournals': get_manual_journals(),
        'Journals': get_journals(),
        'Payments': get_payments(),
        'Receipts': get_receipts(),
        'PurchaseOrders': get_purchase_orders(),
        'Quotes': get_quotes(),
        'Accounts': get_accounts(),
        'Items': get_items(),
        'TaxRates': get_tax_rates(),
        'Currencies': get_currencies(),
        'Attachments': get_attachments(),
        'Budgets': get_budgets(),
        'Organisation': get_organisation(),
        'ProfitLossReport': get_profit_loss_report(),
        'BalanceSheetReport': get_balance_sheet_report(),
        'TrialBalanceReport': get_trial_balance_report()
    }

def get_all_payroll_data():
    """Get all payroll data as a dictionary of DataFrames"""
    print("💼 Retrieving Payroll Data...")
    return {
        'Employees': get_employees(),
        'PayRuns': get_pay_runs(),
        'Timesheets': get_timesheets(),
        'PayrollSettings': get_payroll_settings()
    }

def get_all_other_data():
    """Get assets, files, and projects data"""
    print("📁 Retrieving Other Data...")
    return {
        'Assets': get_assets(),
        'Files': get_files(),
        'Folders': get_folders(),
        'Projects': get_projects()
    }

# =============================================================================
# MAIN EXECUTION FUNCTIONS FOR POWER BI
# =============================================================================

def main():
    """Main function to retrieve all Xero data - Use this in Power BI"""
    print("🚀 Starting Xero API data retrieval for Power BI...")
    
    # Check if tenant ID is set
    if TENANT_ID == "your-tenant-id":
        print("⚠️ Please set your TENANT_ID first!")
        print("Run get_tenant_id() to see available tenants")
        return None
    
    all_data = {}
    
    # Get all data
    try:
        all_data.update(get_all_accounting_data())
        all_data.update(get_all_payroll_data())
        all_data.update(get_all_other_data())
    except Exception as e:
        print(f"Error retrieving data: {str(e)}")
    
    # Print summary
    print("\n📋 Data Summary:")
    for name, df in all_data.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            print(f"{name}: {len(df)} records")
        else:
            print(f"{name}: No data")
    
    print("\n🎉 Data retrieval completed!")
    return all_data

# =============================================================================
# EXECUTION OPTIONS
# =============================================================================

# OPTION 1: Export everything to CSV files (RECOMMENDED)
exported_files = export_all_to_csv()
# exported_files = get_profit_and_loss()


# OPTION 2: Get specific data for Power BI
# df = get_contacts()                    # For contacts
# df = get_invoices()                    # For invoices  
# df = get_bank_transactions()           # For bank transactions

# OPTION 3: Get all data in memory
# all_data = main()

if __name__ == "__main__":
    print("Xero API Script loaded. Available functions:")
    print("- get_tenant_id(): Get your tenant IDs")
    print("- export_all_to_csv(): Export all data to CSV files (RECOMMENDED)")
    print("- export_invoice_summaries(): Export only invoice summaries (FAST)")
    print("- main(): Get all Xero data in memory")
    print("- get_contacts(): Get contacts only")
    print("- get_invoices(): Get invoices only")
    print("- get_bank_transactions(): Get bank transactions only")
    print("- And many more specific functions...")
    print(f"\n📅 Date filter: Data since {DATE_FILTER}")
    print("\n💡 To export all data to CSV files, run: export_all_to_csv()")
    print("💡 For just invoice summaries (faster), run: export_invoice_summaries()")
    print("💡 To get specific data, call any of the get_* functions")

# =============================================================================
# SPECIFIC FUNCTIONS FOR POWER BI QUERIES
# =============================================================================

# For Power BI, you can call specific functions:
# Example: contacts_df = get_contacts()
# Example: transactions_df = get_bank_transactions()

# Or get all data at once:
# all_xero_data = main()

# =============================================================================
# POWER BI EXECUTION - CHOOSE ONE OF THE OPTIONS BELOW
# =============================================================================

# OPTION 1: Get specific data (recommended for Power BI)
# Uncomment the line for the data you want:

df = get_contacts()                    # For contacts
df = get_bank_transactions()           # For bank transactions  
df = get_accounts()                    # For chart of accounts
df = get_employees()                   # For employees

# OPTION 2: Get all accounting data
# accounting_data = get_all_accounting_data()
# df = accounting_data['Contacts']       # Choose which table you want

# OPTION 3: Get everything (may be slow)
# all_data = main()

if __name__ == "__main__":
    print("Xero API Script loaded. Available functions:")
    print("- get_tenant_id(): Get your tenant IDs")
    print("- main(): Get all Xero data")
    print("- get_contacts(): Get contacts only")
    print("- get_bank_transactions(): Get bank transactions only")
    print("\nFinancial Statements:")
    print("- get_profit_and_loss(start_date, end_date): Get P&L statement")
    print("- get_balance_sheet(balance_date): Get Balance Sheet")
    print("- get_cash_flow(start_date, end_date): Get Cash Flow statement")
    print("\nExample usage:")
    print("  # Get P&L for Q1 2025")
    print("  pnl = get_profit_and_loss('2025-01-01', '2025-03-31')")
    print("  # Get Balance Sheet as of today")
    print("  balance_sheet = get_balance_sheet()")
    print("\n💡 To get data, uncomment one of the lines above or call a function directly.")