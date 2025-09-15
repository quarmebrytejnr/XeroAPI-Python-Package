
import os
import pandas as pd
from dotenv import load_dotenv
from xero_python.api_client import ApiClient
from xero_python.api_client.configuration import Configuration
from xero_python.api_client.oauth2 import OAuth2Token
from xero_python.accounting import AccountingApi
from xero_python.payrollau import PayrollAuApi # Note: AU payroll, change if you use a different region (e.g., PayrollUkApi, PayrollNzApi)
from xero_python.exceptions import AccountingBadRequestException
from supabase_config import supabase_config
from token_manager import get_xero_oauth2_token, refresh_xero_oauth2_token

load_dotenv()

def get_xero_tenant_id(api_client):
    """Gets the Xero tenant ID."""
    token = get_xero_oauth2_token()
    if not token:
        raise Exception("Xero token not found. Please run get_token.py first.")

    from xero_python.identity import IdentityApi
    identity_api = IdentityApi(api_client)
    for connection in identity_api.get_connections():
        if connection.tenant_type == "ORGANISATION":
            return connection.tenant_id
    raise Exception("No active organisation tenant found.")

def fetch_all_records(api_call, tenant_id, paginated=True, **kwargs):
    """Fetches all records from a Xero endpoint, with or without pagination."""
    records = []
    if paginated:
        page = 1
        while True:
            try:
                if 'payroll' in str(api_call.__self__.__class__).lower():
                    result = api_call(xero_tenant_id=tenant_id, page=page, **kwargs)
                else:
                    result = api_call(xero_tenant_id=tenant_id, page=page, **kwargs)

                data = result.to_dict()
                
                items_key = None
                for key, value in data.items():
                    if isinstance(value, list):
                        items_key = key
                        break
                
                if not items_key or not data[items_key]:
                    break

                records.extend(data[items_key])
                page += 1
            except AccountingBadRequestException as e:
                print(f"Error fetching page {page}: {e}")
                break
            except Exception as e:
                if '404' in str(e):
                    print(f"Could not fetch from {api_call.__name__}. This can happen if the API is not enabled for your region.")
                    return []
                print(f"An unexpected error occurred during fetch: {e}")
                break
    else:
        try:
            if 'payroll' in str(api_call.__self__.__class__).lower():
                result = api_call(xero_tenant_id=tenant_id, **kwargs)
            else:
                result = api_call(xero_tenant_id=tenant_id, **kwargs)
            data = result.to_dict()
            items_key = None
            for key, value in data.items():
                if isinstance(value, list):
                    items_key = key
                    break
            if items_key:
                records.extend(data[items_key])
        except Exception as e:
            print(f"An unexpected error occurred during fetch: {e}")

    return records

def main():
    """Main function to run the Xero to Supabase pipeline."""
    # Initialize Supabase
    if not supabase_config.initialize():
        return

    # Get Xero token
    token = get_xero_oauth2_token()
    if not token:
        print("Xero token not found. Please run get_token.py to authenticate.")
        return

    # Refresh token if necessary
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    
    try:
        token = refresh_xero_oauth2_token(client_id, client_secret, token)
    except Exception as e:
        print(f"Failed to refresh token: {e}")
        print("Please run get_token.py again to get a new token.")
        return

    # Initialize Xero API client
    api_client = ApiClient(
        Configuration(
            debug=False,
            oauth2_token=OAuth2Token(client_id=client_id, client_secret=client_secret)
        ),
        oauth2_token_getter=lambda: token,
    )
    accounting_api = AccountingApi(api_client)
    payroll_api = PayrollAuApi(api_client) # Using AU payroll

    try:
        xero_tenant_id = get_xero_tenant_id(api_client)
    except Exception as e:
        print(e)
        return

    # Define endpoints to fetch from Xero
    endpoints = {
        'contacts': {'func': accounting_api.get_contacts, 'pk': 'contact_id', 'paginated': True},
        'invoices': {'func': accounting_api.get_invoices, 'pk': 'invoice_id', 'paginated': True},
        'accounts': {'func': accounting_api.get_accounts, 'pk': 'account_id', 'paginated': False},
        'bank_transactions': {'func': accounting_api.get_bank_transactions, 'pk': 'bank_transaction_id', 'paginated': True},
        'journals': {'func': accounting_api.get_journals, 'pk': 'journal_id', 'paginated': False},
        'purchase_orders': {'func': accounting_api.get_purchase_orders, 'pk': 'purchase_order_id', 'paginated': True},
        'manual_journals': {'func': accounting_api.get_manual_journals, 'pk': 'manual_journal_id', 'paginated': True},
        'payments': {'func': accounting_api.get_payments, 'pk': 'payment_id', 'paginated': True},
        'bank_transfers': {'func': accounting_api.get_bank_transfers, 'pk': 'bank_transfer_id', 'paginated': False},
        'organisations': {'func': accounting_api.get_organisations, 'pk': 'organisation_id', 'paginated': False},
        'items': {'func': accounting_api.get_items, 'pk': 'item_id', 'paginated': True},
        'currencies': {'func': accounting_api.get_currencies, 'pk': 'code', 'paginated': False},
        # Payroll endpoints (ensure you have the correct scopes and region)
        'employees': {'func': payroll_api.get_employees, 'pk': 'employee_id', 'paginated': True},
        'pay_runs': {'func': payroll_api.get_pay_runs, 'pk': 'pay_run_id', 'paginated': True},
        'payslip': {'func': payroll_api.get_payslip, 'pk': 'payslip_id', 'paginated': False},
        'timesheets': {'func': payroll_api.get_timesheets, 'pk': 'timesheet_id', 'paginated': True},
    }

    # Create export directory if it doesn't exist
    export_dir = 'xero_exports'
    os.makedirs(export_dir, exist_ok=True)

    for endpoint_name, details in endpoints.items():
        print(f"--- Fetching {endpoint_name} ---")
        try:
            # Special handling for payslip which requires a pay_run_id
            if endpoint_name == 'payslip':
                print("Skipping payslip for now as it requires a specific PayRunID. You can modify the script to fetch these.")
                continue

            records = fetch_all_records(details['func'], xero_tenant_id, paginated=details['paginated'])
            
            if not records:
                print(f"No records found for {endpoint_name}.")
                continue

            df = pd.DataFrame(records)
            
            # --- CSV Export Fallback ---
            csv_path = os.path.join(export_dir, f"{endpoint_name}.csv")
            df.to_csv(csv_path, index=False)
            print(f"Successfully exported {len(records)} records to {csv_path}")

            # --- Supabase Upsert ---
            # Expand JSON columns before creating table or upserting
            df_expanded = supabase_config.expand_json_columns(df)
            
            # Create table if it doesn't exist
            supabase_config.create_table_if_not_exist(
                table_name=endpoint_name, 
                df=df_expanded, 
                primary_key=details['pk']
            )
            
            # Prepare and upsert data
            records_to_upsert = supabase_config.prepare_data_for_supabase(df_expanded.to_dict('records'))
            supabase_config.upsert_data(
                table_name=endpoint_name, 
                records=records_to_upsert,
                primary_key=details['pk']
            )

        except Exception as e:
            print(f"Failed to process {endpoint_name}: {e}")
            # Log the error and continue to the next endpoint
            continue

if __name__ == "__main__":
    main()
