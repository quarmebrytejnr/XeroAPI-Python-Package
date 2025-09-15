"""
Export Financial Statements for Q1-Q3 2025
"""
import os
import json
from datetime import datetime
from new_int import (
    get_profit_and_loss,
    get_balance_sheet,
    get_cash_flow,
    save_to_csv
)

def export_financial_statements():
    """Export financial statements for Q1-Q3 2025"""
    # Create exports directory if it doesn't exist
    os.makedirs('xero_exports', exist_ok=True)
    
    # Define date ranges
    q1_start = '2025-01-01'
    q1_end = '2025-03-31'
    q2_start = '2025-04-01'
    q2_end = '2025-06-30'
    q3_start = '2025-07-01'
    q3_end = '2025-09-30'
    
    print("Exporting financial statements for 2025...")
    
    try:
        # 1. Export Quarterly Profit & Loss Statements
        print("\nExporting Profit & Loss statements...")
        for q, (start, end) in enumerate([(q1_start, q1_end), (q2_start, q2_end), (q3_start, q3_end)], 1):
            print(f"  Q{q} 2025 ({start} to {end})")
            pnl = get_profit_and_loss(start, end)
            filename = f"xero_exports/pnl_q{q}_2025.json"
            with open(filename, 'w') as f:
                json.dump(pnl, f, indent=2)
            print(f"    ✓ Saved to {filename}")
        
        # 2. Export Quarterly Balance Sheets
        print("\nExporting Balance Sheets...")
        for q, date in enumerate([q1_end, q2_end, q3_end], 1):
            print(f"  End of Q{q} 2025 ({date})")
            balance_sheet = get_balance_sheet(date)
            filename = f"xero_exports/balance_sheet_q{q}_2025.json"
            with open(filename, 'w') as f:
                json.dump(balance_sheet, f, indent=2)
            print(f"    ✓ Saved to {filename}")
        
        # 3. Export Quarterly Cash Flow Statements
        print("\nExporting Cash Flow statements...")
        for q, (start, end) in enumerate([(q1_start, q1_end), (q2_start, q2_end), (q3_start, q3_end)], 1):
            print(f"  Q{q} 2025 ({start} to {end})")
            cash_flow = get_cash_flow(start, end)
            filename = f"xero_exports/cash_flow_q{q}_2025.json"
            with open(filename, 'w') as f:
                json.dump(cash_flow, f, indent=2)
            print(f"    ✓ Saved to {filename}")
        
        print("\n✅ All financial statements exported successfully!")
        
    except Exception as e:
        print(f"\n❌ Error exporting financial statements: {str(e)}")
        raise

if __name__ == "__main__":
    export_financial_statements()
