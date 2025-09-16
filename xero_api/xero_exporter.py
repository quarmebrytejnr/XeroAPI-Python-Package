import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional
from xero_python.accounting import AccountingApi
from xero_python.payrollau import PayrollAuApi
from xero_python.api_client import ApiClient, Configuration
from xero_python.api_client.oauth2 import OAuth2Token

# Local imports
from xero_client import XeroClient

class XeroExporter:
    def __init__(self):
        self.client = XeroClient()
        self.export_dir = 'xero_exports'
        os.makedirs(self.export_dir, exist_ok=True)

    def _to_dataframe(self, data: List[Any]) -> pd.DataFrame:
        """Convert Xero API response to pandas DataFrame"""
        if not data:
            return pd.DataFrame()
        
        # Handle Decimal serialization
        def json_serial(obj):
            from decimal import Decimal
            if isinstance(obj, Decimal):
                return float(obj)
            raise TypeError(f"Type {type(obj)} not serializable")
        
        # Convert to dict and serialize
        dict_data = [json.loads(json.dumps(item.to_dict(), default=json_serial)) 
                    for item in data]
        
        # Flatten nested structures
        return pd.json_normalize(dict_data, sep='_')

    def _save_to_csv(self, df: pd.DataFrame, filename: str) -> str:
        """Save DataFrame to CSV and return the file path"""
        if df.empty:
            return ""
            
        filepath = os.path.join(self.export_dir, f"{filename}.csv")
        df.to_csv(filepath, index=False)
        return filepath

    def get_financial_reports(self) -> Dict[str, str]:
        """Fetch financial reports from Xero"""
        if not hasattr(self.client, 'tenant_id') or not self.client.tenant_id:
            raise Exception("No tenant ID found. Please authenticate and select an organization first.")
            
        if not self.client.ensure_valid_token():
            raise Exception("Authentication failed. Please run get_token_new.py first.")
        
        results = {}
        
        # Define the reports to fetch
        report_names = [
            'ProfitAndLoss',
            'AgedReceivablesByContact',
            'AgedPayablesByContact',
            'BalanceSheet',
            'BankSummary',
            'ExecutiveSummary'
        ]
        
        for report_name in report_names:
            try:
                # Get the report
                report = self.client.accounting_api.get_report(
                    xero_tenant_id=self.client.tenant_id,
                    report_id_or_name=report_name
                )
                
                # Convert report to DataFrame
                if hasattr(report, 'reports') and report.reports:
                    report_data = report.reports[0]
                    rows = []
                    
                    # Process report rows
                    if hasattr(report_data, 'rows') and report_data.rows:
                        for row in report_data.rows:
                            if hasattr(row, 'cells') and row.cells:
                                row_data = {}
                                for i, cell in enumerate(row.cells):
                                    if hasattr(cell, 'value'):
                                        row_data[f'col_{i}'] = cell.value
                                    if hasattr(cell, 'attributes'):
                                        for attr in cell.attributes:
                                            row_data[attr.name] = attr.value
                                rows.append(row_data)
                    
                    # Create and save DataFrame
                    if rows:
                        df = pd.DataFrame(rows)
                        filename = report_name.lower()
                        filepath = self._save_to_csv(df, filename)
                        if filepath:
                            results[report_name] = filepath
                            print(f"Successfully exported {report_name} to {filepath}")
                
            except Exception as e:
                print(f"Error fetching {report_name} report: {e}")
        
        return results

    def get_accounting_data(self) -> Dict[str, str]:
        """Fetch all accounting related data"""
        if not hasattr(self.client, 'tenant_id') or not self.client.tenant_id:
            # If no tenant_id, try to get organizations to select one
            try:
                orgs = self.client.accounting_api.get_organizations()
                if orgs and orgs.organizations:
                    self.client.tenant_id = orgs.organizations[0].tenant_id
            except Exception as e:
                print(f"Error getting organizations: {e}")
                
        if not hasattr(self.client, 'tenant_id') or not self.client.tenant_id:
            raise Exception("No tenant ID found. Please authenticate and select an organization first.")
            
        if not self.client.ensure_valid_token():
            raise Exception("Authentication failed. Please run get_token_new.py first.")
        
        results = {}
        
        # Get financial reports first
        results.update(self.get_financial_reports())
        
        # Accounts
        try:
            accounts = self.client.accounting_api.get_accounts(
                xero_tenant_id=self.client.tenant_id
            ).accounts
            df = self._to_dataframe(accounts)
            results['accounts'] = self._save_to_csv(df, 'accounts')
        except Exception as e:
            print(f"Error fetching accounts: {e}")
        
        # Invoices
        try:
            invoices = self.client.accounting_api.get_invoices(
                xero_tenant_id=self.client.tenant_id
            ).invoices
            df = self._to_dataframe(invoices)
            results['invoices'] = self._save_to_csv(df, 'invoices')
        except Exception as e:
            print(f"Error fetching invoices: {e}")
        
        # Contacts
        try:
            contacts = self.client.accounting_api.get_contacts(
                xero_tenant_id=self.client.tenant_id
            ).contacts
            df = self._to_dataframe(contacts)
            results['contacts'] = self._save_to_csv(df, 'contacts')
        except Exception as e:
            print(f"Error fetching contacts: {e}")
        
        # Bank Transactions
        try:
            bank_transactions = self.client.accounting_api.get_bank_transactions(
                xero_tenant_id=self.client.tenant_id
            ).bank_transactions
            df = self._to_dataframe(bank_transactions)
            results['bank_transactions'] = self._save_to_csv(df, 'bank_transactions')
        except Exception as e:
            print(f"Error fetching bank transactions: {e}")
        
        # Manual Journals
        try:
            manual_journals = self.client.accounting_api.get_manual_journals(
                xero_tenant_id=self.client.tenant_id
            ).manual_journals
            df = self._to_dataframe(manual_journals)
            results['manual_journals'] = self._save_to_csv(df, 'manual_journals')
        except Exception as e:
            print(f"Error fetching manual journals: {e}")
        
        return results

    def get_payroll_data(self) -> Dict[str, str]:
        """Fetch all payroll related data"""
        if not self.client.ensure_valid_token():
            raise Exception("Authentication failed. Please run get_token_new.py first.")
            
        results = {}
        
        # Employees
        try:
            employees = self.client.payroll_api.get_employees(
                xero_tenant_id=self.client.token.tenant_id
            ).employees
            df = self._to_dataframe(employees)
            results['employees'] = self._save_to_csv(df, 'employees')
        except Exception as e:
            print(f"Error fetching employees: {e}")
            
        # Pay Runs
        try:
            pay_runs = self.client.payroll_api.get_pay_runs(
                xero_tenant_id=self.client.token.tenant_id
            ).pay_runs
            df = self._to_dataframe(pay_runs)
            results['pay_runs'] = self._save_to_csv(df, 'pay_runs')
        except Exception as e:
            print(f"Error fetching pay runs: {e}")
            
        return results

def export_all_data():
    """Export all Xero data to CSV files"""
    exporter = XeroExporter()
    
    print("Exporting accounting data...")
    accounting_results = exporter.get_accounting_data()
    print("Accounting data exported to:", 
          {k: v for k, v in accounting_results.items() if v})
    
    print("\nExporting payroll data...")
    payroll_results = exporter.get_payroll_data()
    print("Payroll data exported to:", 
          {k: v for k, v in payroll_results.items() if v})

if __name__ == "__main__":
    export_all_data()
